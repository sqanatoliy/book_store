#!/usr/bin/env python3
"""books_etl.py (v4)

ETL процес для обробки даних книг з PostgreSQL:
- Extract: читаємо книги з таблиці books де last_updated >= cutoff_date
- Transform: original_price, rounded_price (1 знак), price_category budget/premium, видаляємо price
- Load: записуємо результат у books_processed

Покращення (після фідбеку):
- logging замість print()
- retry з backoff (tenacity) для підключення та запису (тільки transient помилки)
- chunked processing (ETL_CHUNKSIZE) — обробка частинами, щоб не вантажити все в RAM
- idempotent load: перед вставкою видаляємо записи books_processed для book_id, що обробляються (анти-дублікат)
- рекомендований підхід: pandas читає/пише через SQLAlchemy Connection, а не Engine

Запуск:
  python books_etl.py 2025-01-01

ENV:
  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
  DB_SSLMODE (default=require)  # важливо для Neon
  DB_CHANNEL_BINDING (optional) # інколи потрібне для pooled-host в Neon
  ETL_CHUNKSIZE (default=5000)
  LOG_LEVEL (default=INFO)
  ENV_FILE (optional)           # наприклад ENV_FILE=.env.neon

Залежності:
  pip install pandas sqlalchemy psycopg2-binary python-decouple tenacity
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd
from decouple import AutoConfig, Config, RepositoryEnv
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Connection, Engine, URL
from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError
from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger("books_etl")


# ----------------------------
# Config (.env, ENV_FILE)
# ----------------------------
def _load_config() -> Config:
    """Читає змінні з .env або з файлу, вказаного в ENV_FILE."""
    env_file = os.getenv("ENV_FILE")
    if env_file:
        p = Path(env_file)
        if not p.is_file():
            raise FileNotFoundError(f"ENV_FILE вказано, але файл не знайдено: {env_file}")
        return Config(RepositoryEnv(str(p)))

    return AutoConfig(search_path=Path.cwd())


CFG = _load_config()


def setup_logging() -> None:
    """Налаштування базового логування."""
    level = CFG("LOG_LEVEL", default="INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


# ----------------------------
# Helpers
# ----------------------------
def _validate_cli_date(date_str: str) -> datetime:
    """Валідація CLI аргументу YYYY-MM-DD. Повертає datetime (00:00:00)."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError("Невірний формат дати. Очікується YYYY-MM-DD") from e


def _required_env(host: str, name: str, user: str, password: str) -> None:
    """Перевіряє обовʼязкові параметри підключення."""
    missing = [k for k, v in {
        "DB_HOST": host,
        "DB_NAME": name,
        "DB_USER": user,
        "DB_PASSWORD": password,
    }.items() if not v]
    if missing:
        raise ValueError("Не вказана обов'язкова змінна середовища: " + ", ".join(missing))


# ----------------------------
# DB connect (retry only transient)
# ----------------------------
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=20),
    # Retry тільки transient помилки мережі/каналу.
    retry=retry_if_exception_type((OperationalError, InterfaceError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def connect_to_db() -> Engine:
    """Створює SQLAlchemy Engine і перевіряє з'єднання (SELECT 1)."""
    host = CFG("DB_HOST", default="localhost")
    port = CFG("DB_PORT", default="5432")
    name = CFG("DB_NAME", default="books_db")
    user = CFG("DB_USER", default="")
    password = CFG("DB_PASSWORD", default="")
    sslmode = CFG("DB_SSLMODE", default="require")
    channel_binding = CFG("DB_CHANNEL_BINDING", default=None)

    _required_env(host, name, user, password)

    query = {"sslmode": sslmode}
    if channel_binding:
        query["channel_binding"] = channel_binding

    url = URL.create(
        "postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=port,
        database=name,
        query=query,
    )

    engine = create_engine(url, pool_pre_ping=True)

    # Пробне підключення (ловить неправильний пароль / SSL одразу).
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    logger.info("Підключення до бази даних успішне")
    return engine


# ----------------------------
# Extract
# ----------------------------
def extract_books_iter(engine: Engine, cutoff_dt: datetime, *, chunksize: int) -> Iterator[pd.DataFrame]:
    """Читає books чанками, тримаючи Connection відкритим на час ітерації."""
    sql = """
        SELECT book_id, title, price, genre, stock_quantity, last_updated
        FROM books
        WHERE last_updated >= %(cutoff)s
        ORDER BY last_updated ASC, book_id ASC
    """

    conn = engine.connect()
    try:
        for chunk in pd.read_sql_query(sql, conn, params={"cutoff": cutoff_dt}, chunksize=chunksize):
            yield chunk
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Помилка зчитування даних з таблиці books: {e}") from e
    finally:
        conn.close()


def extract_books(engine: Engine, cutoff_dt: datetime) -> pd.DataFrame:
    """Читає всі дані одним DataFrame (fallback, якщо chunksize <= 0)."""
    sql = """
        SELECT book_id, title, price, genre, stock_quantity, last_updated
        FROM books
        WHERE last_updated >= %(cutoff)s
        ORDER BY last_updated ASC, book_id ASC
    """
    try:
        with engine.connect() as conn:
            return pd.read_sql_query(sql, conn, params={"cutoff": cutoff_dt})
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Помилка зчитування даних з таблиці books: {e}") from e


# ----------------------------
# Transform
# ----------------------------
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Перетворення даних згідно бізнес-правил з ТЗ."""
    if df.empty:
        return df

    out = df.copy()
    out["original_price"] = out["price"]
    out["rounded_price"] = out["price"].round(1)
    out["price_category"] = out["rounded_price"].apply(lambda x: "budget" if x < 500 else "premium")
    out = out.drop(columns=["price"])
    return out


# ----------------------------
# Load (idempotent + retry only transient)
# ----------------------------
def _delete_existing_processed(conn: Connection, book_ids: list[int]) -> None:
    """Видаляє існуючі записи з books_processed для конкретних book_id (анти-дублікат)."""
    delete_stmt = text("DELETE FROM books_processed WHERE book_id IN :ids").bindparams(
        bindparam("ids", expanding=True)
    )
    conn.execute(delete_stmt, {"ids": book_ids})


def _insert_processed(conn: Connection, df: pd.DataFrame) -> None:
    """Вставляє дані в books_processed через pandas.to_sql()."""
    df.to_sql(
        "books_processed",
        conn,  # Важливо: Connection, не Engine
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )


def load_data(df: pd.DataFrame, engine: Engine) -> int:
    """Idempotent load у межах транзакції + retry на transient помилки."""
    if df.empty:
        return 0

    cols = ["book_id", "title", "original_price", "rounded_price", "genre", "price_category"]
    to_load = df[cols].copy()

    # Унікальні book_id у поточному чанку.
    book_ids = to_load["book_id"].dropna().astype(int).unique().tolist()
    if not book_ids:
        return 0

    @retry(
        stop=stop_after_attempt(int(CFG("DB_WRITE_ATTEMPTS", default="3"))),
        wait=wait_exponential(multiplier=1, min=1, max=20),
        retry=retry_if_exception_type((OperationalError, InterfaceError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _do_load() -> int:
        # engine.begin() -> транзакція (commit/rollback автоматично)
        with engine.begin() as conn:
            _delete_existing_processed(conn, book_ids)
            _insert_processed(conn, to_load)
        return len(to_load)

    try:
        return _do_load()
    except SQLAlchemyError as e:
        raise RuntimeError(f"Помилка завантаження оброблених даних: {e}") from e
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Неочікувана помилка при завантаженні: {e}") from e


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    setup_logging()

    if len(sys.argv) != 2:
        print("Використання: python books_etl.py YYYY-MM-DD")
        print("Приклад: python books_etl.py 2025-01-01")
        sys.exit(1)

    cutoff_dt = _validate_cli_date(sys.argv[1])

    engine: Optional[Engine] = None
    start_ts = time.time()

    try:
        engine = connect_to_db()

        chunksize = int(CFG("ETL_CHUNKSIZE", default="5000"))
        use_chunks = chunksize > 0

        extracted_total = 0
        loaded_total = 0

        if use_chunks:
            logger.info("Extract in chunks: chunksize=%d", chunksize)
            any_rows = False

            for idx, chunk in enumerate(extract_books_iter(engine, cutoff_dt, chunksize=chunksize), start=1):
                if chunk.empty:
                    continue

                any_rows = True
                extracted_total += len(chunk)

                transformed = transform_data(chunk)
                loaded = load_data(transformed, engine)
                loaded_total += loaded

                logger.info(
                    "Chunk %d processed: extracted=%d loaded=%d (totals: extracted=%d loaded=%d)",
                    idx,
                    len(chunk),
                    loaded,
                    extracted_total,
                    loaded_total,
                )

            if not any_rows:
                logger.info("Витягнуто 0 записів з таблиці books")
                logger.info("Нових книг для обробки за вказану дату не знайдено. Роботу завершено")
                sys.exit(0)

        else:
            df = extract_books(engine, cutoff_dt)
            logger.info("Витягнуто %d записів з таблиці books", len(df))

            if df.empty:
                logger.info("Нових книг для обробки за вказану дату не знайдено. Роботу завершено")
                sys.exit(0)

            transformed = transform_data(df)
            loaded_total = load_data(transformed, engine)
            extracted_total = len(df)
            logger.info("Збережено %d записів в books_processed", loaded_total)

        logger.info(
            "ETL процес завершено успішно (extracted=%d loaded=%d, elapsed=%.2fs)",
            extracted_total,
            loaded_total,
            time.time() - start_ts,
        )

    except ValueError as e:
        logger.error("Помилка вхідних параметрів: %s", e)
        sys.exit(1)
    except Exception as e:  # noqa: BLE001
        logger.exception("При виконанні виникла помилка: %s", e)
        sys.exit(1)
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    main()