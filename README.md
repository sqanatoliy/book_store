# Books ETL Pipeline

Простий **ETL-пайплайн** (Extract → Transform → Load) для PostgreSQL, який:
- читає книги з таблиці `books` за умовою `last_updated >= cutoff_date`
- трансформує дані (округлення ціни до 1 знака + категорія `budget/premium`)
- записує результат у таблицю `books_processed`

---

## Структура проєкту

- `books_schema.sql` — створення таблиць, індексів, тригера та тестових даних (6 рядків)
- `books_etl.py` — ETL-скрипт
- `requirements.txt` — Python-залежності

---

## Вимоги

- Python 3.10+ (рекомендовано)
- PostgreSQL (локально або хмарний, наприклад Neon)
- `psql` (або інший клієнт) для виконання `books_schema.sql`

---

## Встановлення

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## Підготовка бази даних

Є два варіанти: **локальний PostgreSQL** або **Neon (хмара)**.

---

# Варіант A: Локальний PostgreSQL

### 1) Створення бази та користувача (приклад)

```bash
psql -h localhost -U <your_admin_user> -d postgres
```

```sql
CREATE USER books_user WITH PASSWORD 'books_pass';
CREATE DATABASE books_db OWNER books_user;
\q
```

### 2) Створення схеми та тестових даних

```bash
psql -h localhost -U books_user -d books_db -f books_schema.sql
```

Перевірка:

```bash
psql -h localhost -U books_user -d books_db -c "SELECT count(*) FROM books;"
```

Очікувано: `6`.

> У `books_schema.sql` є `TRUNCATE` перед `INSERT`, щоб уникати дублювання при повторних запусках.

---

# Варіант B: Neon (реальна хмарна PostgreSQL)

### 1) Створення проєкту / отримання connection string

1. Створи Project у Neon.
2. На Project Dashboard натисни **Connect**.
3. Обери Branch / Database / Role — Neon згенерує connection string (і команду для `psql`).
4. Якщо пароль для ролі не заданий — натисни **Reset password** у вікні Connect (для обраної ролі).

### 2) Перевір підключення (через psql)

```bash
psql "postgresql://<USER>:<PASSWORD>@<HOST>:5432/<DB>?sslmode=require" -c "SELECT 1;"
```

### 3) Завантаження схеми у Neon

```bash
psql "postgresql://<USER>:<PASSWORD>@<HOST>:5432/<DB>?sslmode=require" -f books_schema.sql
```

Перевірка:

```bash
psql "postgresql://<USER>:<PASSWORD>@<HOST>:5432/<DB>?sslmode=require" -c "SELECT count(*) FROM books;"
```

---

## Конфігурація підключення (.env)

Скрипт читає налаштування через **python-decouple** з `.env` або з environment variables.

### `.env` для локального Postgres

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=books_db
DB_USER=books_user
DB_PASSWORD=books_pass
```

### `.env` для Neon (приклад)

```env
DB_HOST=<HOST_FROM_NEON>
DB_PORT=5432
DB_NAME=<DB_FROM_NEON>
DB_USER=<ROLE_FROM_NEON>
DB_PASSWORD=<PASSWORD_FROM_NEON>
DB_SSLMODE=require

# Опційно: якщо підключаєшся до pooled host (часто містить "-pooler" в hostname)
DB_CHANNEL_BINDING=require
```

### ENV_FILE для різних середовищ (dev/staging/prod)

Можна тримати кілька файлів конфігурації, напр.:

- `.env.local`
- `.env.neon`

І запускати так:

```bash
ENV_FILE=.env.neon python books_etl.py 2025-01-01
```

---

## Запуск ETL

Скрипт приймає один обовʼязковий аргумент — `cutoff_date` у форматі `YYYY-MM-DD`.

```bash
python books_etl.py 2025-01-01
```

---

## Перевірка результату

```bash
psql -h <HOST> -U <USER> -d <DB> -c "SELECT * FROM books_processed ORDER BY processed_id;"
```

---

## Production improvements (з урахуванням фідбеку)

У `books_etl.py` реалізовано такі покращення:

- **Logging**: використовується модуль `logging` (рівень через `LOG_LEVEL`)
- **Retry**: повторні спроби для connect/load з exponential backoff (`DB_CONNECT_ATTEMPTS`, `DB_WRITE_ATTEMPTS`)
- **Обробка дублікатів**: перед вставкою у `books_processed` видаляються існуючі рядки по `book_id` (idempotent load)
- **Масштабування**: підтримується chunked processing через `ETL_CHUNKSIZE` (за замовчуванням 5000)
- **Прогрес**: у режимі чанків логуються підсумки по кожному chunk

Приклади корисних змінних:

```env
LOG_LEVEL=INFO
ETL_CHUNKSIZE=5000
DB_CONNECT_ATTEMPTS=3
DB_WRITE_ATTEMPTS=3
```

---

## Типові проблеми

- **`Не вказана обов'язкова змінна середовища`**  
  Перевір, що `.env` лежить у корені проєкту або використай `ENV_FILE=...`

- **`relation "books" does not exist`**  
  Не виконано `books_schema.sql` або підключення йде не в ту базу.

---

## Залежності

Залежності беруться з `requirements.txt`:
- `python-decouple`
- `pandas`
- `SQLAlchemy`
- `psycopg2-binary`
