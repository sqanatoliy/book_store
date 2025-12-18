
#!/usr/bin/env python3
"""books_etl.py (v3)

ETL процес для обробки даних книг з PostgreSQL:
- Витягнути книги з таблиці books де last_updated >= cutoff_date
- Трансформувати дані згідно бізнес-правил
- Завантажити трансформовані дані в books_processed

Покращення після відгуку:
- logging замість print()
- retry з backoff (конект/запис)
- chunked processing (ETL_CHUNKSIZE)
- idempotent load: перед вставкою видаляємо books_processed по book_id (анти-дублікат)

"""

from __future__ import annotations

import logging
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd
from decouple import AutoConfig, Config, RepositoryEnv
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine, URL
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger("books_etl")


def _load_config() -> Config:
    """
    Підтримує різні середовища через ENV_FILE:
      ENV_FILE=.env.neon python books_etl_v3.py 2025-01-01
    Якщо ENV_FILE не заданий — читає стандартний .env з поточної директорії.
    """
    env_file = os.getenv("ENV_FILE")
    if env_file:
        p = Path(env_file)
        if not p.is_file():
            raise FileNotFoundError(f"ENV_FILE вказано, але файл не знайдено: {env_file}")
        return Config(RepositoryEnv(str(p)))

    return AutoConfig(search_path=Path.cwd())


CFG = _load_config()


def setup_logging() -> None:
    level = CFG("LOG_LEVEL", default="INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def retry(
    fn,
    *,
    attempts: int = 3,
    base_delay: float = 1.0,
    backoff: float = 2.0,
    max_delay: float = 20.0,
    retry_exceptions: tuple[type[BaseException], ...] = (Exception,),
    op_name: str = "operation",
):
    """Проста retry-логіка з exponential backoff + jitter."""
    last_exc: Optional[BaseException] = None
    delay = base_delay

    for i in range(1, attempts + 1):
        try:
            return fn()
        except retry_exceptions as exc:  # noqa: BLE001
            last_exc = exc
            if i == attempts:
                break

            jitter = random.uniform(0, 0.2 * delay)
            sleep_for = min(max_delay, delay + jitter)
            logger.warning(
                "%s failed (attempt %d/%d): %s. Retry in %.2fs",
                op_name,
                i,
                attempts,
                exc,
                sleep_for,
            )
            time.sleep(sleep_for)
            delay *= backoff

    assert last_exc is not None
    raise last_exc


def connect_to_db() -> Engine:
    host = CFG("DB_HOST", default="localhost")
    port = CFG("DB_PORT", default="5432")
    name = CFG("DB_NAME", default="books_db")
    user = CFG("DB_USER", default="")
    password = CFG("DB_PASSWORD", default="")
    sslmode = CFG("DB_SSLMODE", default="require")
    channel_binding = CFG("DB_CHANNEL_BINDING", default=None)

    missing = [k for k, v in {
        "DB_HOST": host,
        "DB_NAME": name,
        "DB_USER": user,
        "DB_PASSWORD": password,
    }.items() if not v]

    if missing:
        raise ValueError("Не вказана обов'язкова змінна середовища: " + ", ".join(missing))

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

    def _connect() -> Engine:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine

    try:
        engine = retry(
            _connect,
            attempts=int(CFG("DB_CONNECT_ATTEMPTS", default="3")),
            base_delay=float(CFG("DB_CONNECT_DELAY", default="1.0")),
            op_name="DB connect",
            retry_exceptions=(SQLAlchemyError, Exception),
        )
        logger.info("Підключення до бази даних успішне")
        return engine
    except SQLAlchemyError as e:
        raise ConnectionError(f"Помилка підключення до БД: {e}") from e


def _validate_cli_date(date_str: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError("Невірний формат дати. Очікується YYYY-MM-DD") from e


def extract_books_iter(engine: Engine, cutoff_dt: datetime, *, chunksize: int) -> Iterator[pd.DataFrame]:
    """
    Витягує книги з таблиці books, чанками.
    Важливо: pyformat плейсхолдер %(cutoff)s (psycopg2).
    """
    sql = """
        SELECT book_id, title, price, genre, stock_quantity, last_updated
        FROM books
        WHERE last_updated >= %(cutoff)s
        ORDER BY last_updated ASC, book_id ASC
    """
    try:
        return pd.read_sql_query(sql, engine, params={"cutoff": cutoff_dt}, chunksize=chunksize)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Помилка зчитування даних з таблиці books: {e}") from e


def extract_books(engine: Engine, cutoff_dt: datetime) -> pd.DataFrame:
    sql = """
        SELECT book_id, title, price, genre, stock_quantity, last_updated
        FROM books
        WHERE last_updated >= %(cutoff)s
        ORDER BY last_updated ASC, book_id ASC
    """
    try:
        return pd.read_sql_query(sql, engine, params={"cutoff": cutoff_dt})
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Помилка зчитування даних з таблиці books: {e}") from e


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    out["original_price"] = out["price"]
    out["rounded_price"] = out["price"].round(1)
    out["price_category"] = out["rounded_price"].apply(lambda x: "budget" if x < 500 else "premium")
    out = out.drop(columns=["price"])
    return out


def load_data(df: pd.DataFrame, engine: Engine) -> int:
    if df.empty:
        return 0

    cols = ["book_id", "title", "original_price", "rounded_price", "genre", "price_category"]
    to_load = df[cols].copy()

    book_ids = to_load["book_id"].dropna().astype(int).unique().tolist()
    if not book_ids:
        return 0

    delete_stmt = text("DELETE FROM books_processed WHERE book_id IN :ids").bindparams(
        bindparam("ids", expanding=True)
    )

    def _do_load() -> int:
        with engine.begin() as conn:
            conn.execute(delete_stmt, {"ids": book_ids})
            to_load.to_sql(
                "books_processed",
                conn,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )
        return len(to_load)

    try:
        return retry(
            _do_load,
            attempts=int(CFG("DB_WRITE_ATTEMPTS", default="3")),
            base_delay=float(CFG("DB_WRITE_DELAY", default="1.0")),
            op_name="DB load",
            retry_exceptions=(SQLAlchemyError, Exception),
        )
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Помилка завантаження оброблених даних: {e}") from e


def main() -> None:
    setup_logging()

    if len(sys.argv) != 2:
        print("Використання: python books_etl_v3.py YYYY-MM-DD")
        print("Приклад: python books_etl_v3.py 2025-01-01")
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
            logger.info("Збережено %d записів в books_processed", loaded_total)

        logger.info("ETL процес завершено успішно (loaded=%d, elapsed=%.2fs)", loaded_total, time.time() - start_ts)

    except Exception as e:  # noqa: BLE001
        logger.exception("При виконанні виникла помилка: %s", e)
        sys.exit(1)
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    main()
