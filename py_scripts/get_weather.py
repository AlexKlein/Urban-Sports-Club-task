"""
ETL process for getting weather forecast and load it in PostgreSQL database.

"""
import sys
from datetime import datetime, timedelta

import requests
from psycopg2 import connect, sql, DatabaseError
from pytz import timezone


APP_ID = 'e7b27d7b1549464dbed6936feb1d54d0'
CITY = 'Berlin'
COUNTRY = 'DE'

DATABASE_NAME = 'postgres'
DATABASE_USER = 'postgres'
DATABASE_PASSWORD = 'system'


def get_city_id() -> int:
    try:
        data_set = requests.get(
            "http://api.openweathermap.org/data/2.5/find",
            params={
                'q': CITY + ',' + COUNTRY,
                'type': 'like',
                'units': 'metric',
                'APPID': APP_ID
            }
        ).json()

        city_id = data_set["list"][0]["id"]

    except Exception as e:
        print("You've got API connection error:", e)
        sys.exit(1)

    return city_id


def get_weather_forecast(city_id) -> dict:
    try:
        data_set = requests.get(
            "http://api.openweathermap.org/data/2.5/weather",
            params={
                'id': city_id,
                'units': 'metric',
                'lang': 'ru',
                'APPID': APP_ID
            }
        ).json()
    except Exception as e:
        print("You've got API connection error", e)
        sys.exit(1)

    return data_set


def check_connect() -> connect:
    try:
        conn = connect(
            dbname=DATABASE_NAME,
            user=DATABASE_USER,
            password=DATABASE_PASSWORD
        )
        conn.autocommit = True
    except DatabaseError as e:
        print("You've got Database Error", e.pgerror)
        sys.exit(1)

    return conn


def upload_data_to_database(data_set, conn):
    current_date = datetime.date(
        datetime.now() +
        timedelta(days=1)
    )
    delete_query = (
        "delete from weather_fct "
        "where value_day = date'{date}' and "
        "city = '{city}' and "
        "country = '{country}'"
    ).format(
        date=current_date.strftime("%Y-%m-%d"),
        city=CITY,
        country=COUNTRY
    )
    pg_cursor = conn.cursor()
    pg_cursor.execute(delete_query)
    print("Deleted {} rows.".format(pg_cursor.rowcount))

    pg_cursor.execute(
        sql.SQL("insert into {} values "
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)").format(
            sql.Identifier('weather_fct')),
        [
            CITY,
            COUNTRY,
            datetime.date(datetime.now() + timedelta(days=1)),
            data_set['weather'][0]['description'],
            data_set['main']['temp'],
            data_set['main']['pressure'],
            data_set['main']['humidity'],
            data_set['main']['temp_min'],
            data_set['main']['temp_max'],
            datetime.now(tz=timezone('Europe/Berlin')),
            'openweathermap'
        ]
    )

    print("Inserted {} rows.".format(pg_cursor.rowcount))


if __name__ == '__main__':
    city_id = get_city_id()
    data_set = get_weather_forecast(city_id)
    connection = check_connect()
    upload_data_to_database(data_set, connection)
    connection.commit()
