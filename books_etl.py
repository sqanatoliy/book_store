#!/usr/bin/env python3
"""books_etl.py
ETL процес для обробки даних книг з PostgreSQL:
- Витягнути книги з таблиці books де last_updated >= cutoff_date
- Трансформувати дані згідно бізнес-правил
- Завантажити трансформовані дані в books_processed

Залежності:
    pip install pandas sqlalchemy psycopg2-binary python-decouple
"""

from __future__ import annotations

import sys
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL
from sqlalchemy.exc import SQLAlchemyError
from decouple import config


def connect_to_db() -> Engine:
    """
    SQLAlchemy engine для підключення до PostgreSQL
    Використовує environment variables для параметрів підключення
    Повертає engine об'єкт
    """

    host = config("DB_HOST", default="localhost")
    port = config("DB_PORT", default="5432")
    name = config("DB_NAME", default="books_db")
    user = config("DB_USER", default="")
    password = config("DB_PASSWORD", default="")

    missing = [k for k, v in {
        "DB_HOST": host,
        "DB_NAME": name,
        "DB_USER": user,
        "DB_PASSWORD": password,
    }.items() if not v]


    if missing:
        raise ValueError("Не вказана обов'язкова змінна середовища: " + ", ".join(missing))

    url = URL.create(
        "postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=port,
        database=name
    )

    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Підключення до бази даних успішне")
        return engine
    except SQLAlchemyError as e:
        raise ConnectionError(f"Помилка підключення до БД: {e}") from e


def extract_books(engine: Engine, cutoff_date: str) -> pd.DataFrame:
    """Зчитує дані з таблиці books починаючи з cutoff_date (YYYY-MM-DD)."""
    try:
        cutoff_dt = datetime.strptime(cutoff_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError("cutoff_date повинно бути у форматі YYYY-MM-DD") from e

    query = text("""
        SELECT book_id, title, price, genre, stock_quantity, last_updated
        FROM books
        WHERE last_updated >= :cutoff
        ORDER BY last_updated ASC, book_id ASC
    """)

    try:
        df = pd.read_sql_query(query, engine, params={"cutoff": cutoff_dt})
        print(f"Витягнуто {len(df)} записів з таблиці books")
        return df
    except Exception as e:
        raise RuntimeError(f"Помилка зчитування даних з таблиці books: {e}") from e


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Трансформувати дані згідно бізнес-правил:
    - Додати колонку original_price (оригінальна ціна)
    - Округлити ціну до 1 знаку після коми і зберегти в rounded_price
    - Додати колонку price_category: "budget" якщо rounded_price < 500, інакше "premium"
    - Видалити колонку price
    """
    if df.empty:
        print("Дані відсутні, пропускаємо обробку")
        return df

    out = df.copy()
    out["original_price"] = out["price"]
    out["rounded_price"] = out["price"].round(1)
    out["price_category"] = out["rounded_price"].apply(lambda x: "budget" if x < 500 else "premium")
    out = out.drop(columns=["price"])

    print(f"Трансформовано {len(out)} записів")
    return out


def load_data(df: pd.DataFrame, engine: Engine) -> int:
    """Записує оброблені дані в таблицю books_processed (append)"""
    if df.empty:
        return 0

    cols = ["book_id", "title", "original_price", "rounded_price", "genre", "price_category"]
    to_load = df[cols].copy()

    try:
        to_load.to_sql(
            "books_processed",
            engine,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )
        print(f"Збережено {len(to_load)} записів в books_processed")
        return len(to_load)
    except Exception as e:
        raise RuntimeError(f"Помилка завантаження оброблених даних: {e}") from e


def _validate_cli_date(date_str: str) -> str:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Невірний формат дати. Очікується YYYY-MM-DD")
    return date_str


def main() -> None:
    if len(sys.argv) != 2:
        print("Використання: python books_etl.py YYYY-MM-DD")
        print("Приклад: python books_etl.py 2025-01-01")
        sys.exit(1)

    cutoff_date = _validate_cli_date(sys.argv[1])

    try:
        engine = connect_to_db()
        df = extract_books(engine, cutoff_date)

        if df.empty:
            print("Нових книг для обробки за вказану дату не знайдено. Роботу завершено")
            sys.exit(0)

        transformed = transform_data(df)
        loaded = load_data(transformed, engine)

        if loaded > 0:
            print("ETL процес завершено успішно")

    except Exception as e:
        print(f"При виконанні виникла неочікувана помилка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
