import os
import pandas as pd
from sqlalchemy import create_engine
from time import time
import click
from tqdm import tqdm

DTYPE = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

PARSE_DATES = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

@click.command()
@click.option('--user', required=True, help='PostgreSQL user')
@click.option('--password', required=True, help='PostgreSQL password')
@click.option('--host', required=True, help='PostgreSQL host')
@click.option('--port', required=True, help='PostgreSQL port')
@click.option('--db', required=True, help='PostgreSQL database name')
@click.option('--table_name', required=True, help='Target table name')
@click.option('--url', required=True, help='URL of the csv file')
def main(user, password, host, port, db, table_name, url):
    
    # Создаем подключение
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    print(f"Начинаем загрузку данных из {url}...")

    # Читаем CSV по ссылке
    df_iter = pd.read_csv(
        url, 
        dtype=DTYPE, 
        parse_dates=PARSE_DATES, 
        iterator=True, 
        chunksize=100000
    )

    first_chunk = True

    # Запускаем цикл с прогресс-баром
    with tqdm(unit='chunk') as pbar:
        for df in df_iter:
            if first_chunk:
                # Создаем таблицу (удаляем старую, если есть)
                df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
                print(f" Таблица '{table_name}' создана.")
                first_chunk = False

            # Вставляем данные
            df.to_sql(name=table_name, con=engine, if_exists='append')
            
            pbar.update(1)

    print(f"Загрузка завершена! Данные в таблице '{table_name}'.")

if __name__ == '__main__':
    main()