import psycopg2
from psycopg2 import sql
import time
import random
from datetime import datetime
import os

# конфигурация БД
DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'weather_db')
DB_USER = os.getenv('DB_USER', 'weather_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')


def connect_db():
    #Подключение к БД с повторными попытками
    max_retries = 10
    retry_count = 0

    while retry_count < max_retries:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            print("Подключение к БД успешно!")
            return conn
        except psycopg2.OperationalError as e:
            retry_count += 1
            print(f"Попытка подключения {retry_count}/{max_retries}. Ошибка: {e}")
            time.sleep(2)

    raise Exception("Не удалось подключиться к БД")


def init_database(conn):
    #Инициализация таблицы
    cur = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_readings (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        temperature DECIMAL(5,2) NOT NULL,
        humidity DECIMAL(5,2) NOT NULL,
        pressure DECIMAL(7,2) NOT NULL,
        wind_speed DECIMAL(5,2) NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_timestamp ON weather_readings(timestamp);
    """

    cur.execute(create_table_query)
    conn.commit()
    print("Таблица инициализирована!")
    cur.close()


def generate_weather_data():
    # диапазоны для Владивостока
    temp = random.uniform(-25, 35)  # Температура от -25 до +35°C
    humidity = random.uniform(20, 100)  # Влажность 20-100%
    pressure = random.uniform(990, 1030)  # Давление 990-1030 мбар
    wind_speed = random.uniform(0, 20)  # Скорость ветра 0-20 м/с

    # если холодно, влажность выше
    if temp < 0:
        humidity = min(100, humidity + 15)

    return {
        'temperature': round(temp, 2),
        'humidity': round(humidity, 2),
        'pressure': round(pressure, 2),
        'wind_speed': round(wind_speed, 2)
    }


def insert_weather_data(conn, data):
    #Вставка данных в БД
    cur = conn.cursor()

    insert_query = sql.SQL("""
        INSERT INTO weather_readings (temperature, humidity, pressure, wind_speed)
        VALUES (%s, %s, %s, %s)
    """)

    cur.execute(insert_query, (
        data['temperature'],
        data['humidity'],
        data['pressure'],
        data['wind_speed']
    ))

    conn.commit()
    cur.close()

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Данные вставлены: T={data['temperature']}°C, H={data['humidity']}%, P={data['pressure']} мбар, V={data['wind_speed']} м/с")


def main():
    print("Генератор данных погодной станции запущен...")

    conn = connect_db()
    init_database(conn)

    try:
        counter = 0
        while True:
            data = generate_weather_data()
            insert_weather_data(conn, data)

            counter += 1
            print(f"   Всего записей: {counter}")

            time.sleep(1)  # Одна запись в секунду

    except KeyboardInterrupt:
        print("\n Генератор остановлен пользователем")
    finally:
        conn.close()
        print(" Соединение закрыто")


if __name__ == '__main__':
    main()
