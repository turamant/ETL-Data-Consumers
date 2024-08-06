"""_package my_dagster"""

from dagster import op, job, DagsterInstance
import sqlalchemy
from pymongo import MongoClient
import pandas as pd


# op to extract data from PostgreSQL
@op
def extract_postgres():
    engine = sqlalchemy.create_engine('postgresql+asyncpg://username:password@localhost/mydatabase')
    with engine.connect() as connection:
        result = pd.read_sql("SELECT * FROM offline_customers", connection)
    return result

# op to extract data from MongoDB
@op
def extract_mongo():
    client = MongoClient('mongodb://localhost:27017/')
    db = client["online_customers_db"]
    collection = db["online_customers"]
    data = pd.DataFrame(list(collection.find()))
    return data

# op to transform the data
@op
def transform_data(data_from_postgres, data_from_mongo):
    # Пусть для примера форматируем в один датафрейм
    data_from_mongo = data_from_mongo.rename(columns={
        "name": "customer_name",
        "purchase": "customer_purchase"
    })
    data_from_postgres = data_from_postgres.rename(columns={
        "client_name": "customer_name",
        "order_detail": "customer_purchase"
    })
    combined_data = pd.concat([data_from_postgres, data_from_mongo], ignore_index=True)
    return combined_data

# op to load the data into a new PostgreSQL table
@op
def load_data(transformed_data):
    engine = sqlalchemy.create_engine('postgresql://username:password@localhost/new_database')
    transformed_data.to_sql('combined_customers', engine, if_exists='replace')

@job
def etl_job():
    postgres_data = extract_postgres()
    mongo_data = extract_mongo()
    transformed_data = transform_data(postgres_data, mongo_data)
    load_data(transformed_data)

instance = DagsterInstance.get()

# Для запуска ETL процесса
if __name__ == '__main__':
    result = etl_job.execute_in_process(instance=instance)
    if result.success:
        print("ETL Process Succeeded!")
    else:
        print("ETL Process Failed!")
