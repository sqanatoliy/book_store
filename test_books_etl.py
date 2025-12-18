import pytest
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

# Імпортуємо функції з вашого скрипта
# (переконайтесь, що ваш файл називається books_etl.py)
from books_etl import transform_data, _validate_cli_date, _required_env, load_data


# -------------------------------------------------------------------
# Fixtures (Дані для тестів)
# -------------------------------------------------------------------
@pytest.fixture
def input_df():
    """Створює тестовий DataFrame, що імітує дані з БД."""
    data = {
        "book_id": [1, 2, 3],
        "title": ["Cheap Book", "Expensive Book", "Borderline Book"],
        "price": [100.00, 999.99, 499.95],  # 499.95 округлиться до 500.0? Перевіримо.
        "genre": ["Fiction", "Tech", "History"],
        "stock_quantity": [10, 5, 2],
        "last_updated": [datetime(2025, 1, 1), datetime(2025, 1, 1), datetime(2025, 1, 1)],
    }
    return pd.DataFrame(data)

# -------------------------------------------------------------------
# Tests for Transformation (Бізнес-логіка)
# -------------------------------------------------------------------
def test_transform_data_logic(input_df):
    """Перевіряємо, чи правильно рахуються категорії та округлення."""
    
    # Виконуємо функцію
    result = transform_data(input_df)

    # 1. Перевіряємо, чи зникла колонка 'price'
    assert "price" not in result.columns
    assert "original_price" in result.columns
    assert "rounded_price" in result.columns

    # 2. Перевіряємо округлення (499.95 -> 500.0 ?)
    # Примітка: round(1) працює як банківське округлення або математичне, залежить від версії.
    # В Pandas .round(1) для 499.95 зазвичай дає 500.0 (float).
    expected_rounded = [100.0, 1000.0, 500.0] 
    # Але чекайте, 999.99 round(1) -> 1000.0. 
    # Давайте перевіримо конкретні значення, які ми очікуємо.
    
    assert result.loc[0, "rounded_price"] == 100.0
    assert result.loc[1, "rounded_price"] == 1000.0
    
    # 3. Перевіряємо категорії (Budget < 500, Premium >= 500)
    # 100.0 -> budget
    # 1000.0 -> premium
    # 500.0 -> premium (оскільки умова x < 500)
    assert result.loc[0, "price_category"] == "budget"
    assert result.loc[1, "price_category"] == "premium"
    # Якщо 499.95 округлилось до 500.0, то це Premium
    assert result.loc[2, "price_category"] == "premium"

def test_transform_data_empty():
    """Перевіряємо, що пустий вхід дає пустий вихід, а не помилку."""
    empty_df = pd.DataFrame()
    result = transform_data(empty_df)
    assert result.empty
    
def test_transform_data_structure(input_df):
    """Перевіряємо точну відповідність структури DataFrame (schema)."""
    result = transform_data(input_df)
    expected_columns = ["book_id", "title", "genre", "stock_quantity", 
                        "last_updated", "original_price", "rounded_price", "price_category"]
    
    # Перевіряємо, що всі колонки на місці (порядок може відрізнятись, тому set)
    assert set(result.columns) == set(expected_columns)

# -------------------------------------------------------------------
# Tests for Validation Helpers
# -------------------------------------------------------------------
def test_validate_cli_date_valid():
    """Перевірка валідної дати."""
    dt = _validate_cli_date("2025-01-01")
    assert isinstance(dt, datetime)
    assert dt == datetime(2025, 1, 1)

def test_validate_cli_date_invalid_format():
    """Перевірка неправильного формату."""
    with pytest.raises(ValueError, match="Невірний формат дати"):
        _validate_cli_date("01-01-2025")  # DD-MM-YYYY не підтримуємо

def test_validate_cli_date_garbage():
    """Перевірка сміття на вході."""
    with pytest.raises(ValueError):
        _validate_cli_date("not-a-date")

def test_required_env_missing():
    """Перевірка, що функція кидає помилку, якщо немає змінних."""
    with pytest.raises(ValueError, match="Не вказана обов'язкова змінна"):
        _required_env("localhost", "", "user", "pass") # DB_NAME пустий

def test_required_env_ok():
    """Якщо все є, помилки немає."""
    try:
        _required_env("localhost", "db", "user", "pass")
    except ValueError:
        pytest.fail("_required_env raised ValueError unexpectedly!")

# -------------------------------------------------------------------
# тестування Load Data з реальною БД
# -------------------------------------------------------------------

def test_load_data_real_db_flow(input_df):
    """
    Тестуємо load_data за допомогою справжньої бази SQLite в пам'яті.
    Це перевіряє логіку транзакцій, видалення та вставки.
    """
    # 1. Створюємо реальний engine для SQLite в пам'яті
    engine = create_engine("sqlite:///:memory:")
    
    # 2. Створюємо необхідну таблицю
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE books_processed (
                book_id INTEGER PRIMARY KEY,
                title TEXT,
                original_price REAL,
                rounded_price REAL,
                genre TEXT,
                price_category TEXT
            )
        """))
        # Додамо "старий" запис для перевірки ідемпотентності (видалення перед записом)
        conn.execute(text("""
            INSERT INTO books_processed (book_id, title) VALUES (1, 'Old Title')
        """))

    # 3. Підготовка даних (3 книги, одна з яких має id=1)
    processed_df = transform_data(input_df)
    
    # 4. Виконуємо load_data
    loaded_count = load_data(processed_df, engine)
    
    # 5. Перевірки
    assert loaded_count == 3
    
    with engine.connect() as conn:
        # Перевіряємо загальну кількість
        res = conn.execute(text("SELECT COUNT(*) FROM books_processed")).scalar()
        assert res == 3
        
        # Перевіряємо, чи оновився запис з id=1 (чи спрацював DELETE перед INSERT)
        title_res = conn.execute(
            text("SELECT title FROM books_processed WHERE book_id = 1")
        ).scalar()
        assert title_res == "Cheap Book"  # Нова назва, а не 'Old Title'