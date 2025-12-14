
-- Налаштування середовища для безпеки та передбачуваності, щоб ваш код працював однаково на різних комп'ютерах
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;


-- TODO 1: Створіть основну таблицю books

CREATE TABLE IF NOT EXISTS books (
    -- book_id: ми також могли використати BIGSERIAL або інші способи але в завданні була вказано SERIAL
    book_id         SERIAL PRIMARY KEY,
    title           VARCHAR(500) NOT NULL,
    price           NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    genre           VARCHAR(100),
    stock_quantity  INTEGER NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    last_updated    TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- OPTIONAL: можливо варто автоматично оновлювати last_updated при зміні рядка інакше це завжди буде відображати час створення

CREATE OR REPLACE FUNCTION set_last_updated_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS set_books_timestamp ON books;
CREATE TRIGGER set_books_timestamp
BEFORE UPDATE ON books
FOR EACH ROW
EXECUTE FUNCTION set_last_updated_timestamp();


-- TODO 2: Створіть таблицю для обробленх даних books_processed

CREATE TABLE IF NOT EXISTS books_processed (
    processed_id    SERIAL PRIMARY KEY,
    book_id         INTEGER NOT NULL,
    title           VARCHAR(500) NOT NULL,
    original_price  NUMERIC(10, 2) NOT NULL CHECK (original_price >= 0),
    rounded_price   NUMERIC(10, 1) NOT NULL CHECK (rounded_price >= 0),
    genre           VARCHAR(100),
    price_category  VARCHAR(10) NOT NULL CHECK (price_category IN ('budget', 'premium')),
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()

    -- Якщо потрібно забезпечити цілісність даних між books і books_processed, можна додати зовнішній ключ (FOREIGN KEY) :
    -- ,CONSTRAINT fk_books_processed_book
    --    FOREIGN KEY (book_id) REFERENCES books(book_id)
);


-- TODO 3: Створіть індекси для оптимізації

CREATE INDEX IF NOT EXISTS idx_books_genre ON books (genre);
CREATE INDEX IF NOT EXISTS idx_books_last_updated ON books (last_updated);
CREATE INDEX IF NOT EXISTS idx_books_price_range ON books (price);


-- TODO 4: Додайте тестові дані
-- Вставте рівно 6 записів книг з наступними вимогами:
--  - Мінімум 3 різних жанри
--  - Ціни від 200 до 800 грн (щоб протестувати категорії 'budget'/'premium')
--  - Різні дати last_updated
--  - Різну кількість stock_quantity

-- Щоб уникнути дублювання даних при повторному запуску sql скрипта, спочатку очищуємо таблиці
-- Цей крок не був вказаний в завданні, але є корисним для тестування, я запускав цей скрипт кілька разів
TRUNCATE TABLE books_processed RESTART IDENTITY;
TRUNCATE TABLE books RESTART IDENTITY;

INSERT INTO books (title, price, genre, stock_quantity, last_updated) VALUES
('Тіні забутих предків',              299.99, 'Класика',     15, '2025-01-15 10:30:00'),
('Книга-рецепт: Борщ',                200.00, 'Кулінарія',    5, '2025-02-01 12:00:00'),
('Трилогія: Хроніки Дюни',            750.50, 'Фантастика',  22, '2025-03-20 08:00:00'),
('Посібник Python для Data Science',  610.25, 'Освіта',      40, '2025-04-10 15:45:00'),
('Аліса в Дивокраї',                  380.00, 'Казка',       10, '2025-05-05 18:30:00'),
('1984',                              450.75, 'Фантастика',  30, '2025-06-01 09:10:00');
