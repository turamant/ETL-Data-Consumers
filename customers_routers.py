from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
import sqlalchemy
import pandas as pd

customer_router = APIRouter()

class CustomerData(BaseModel):
    name: str
    email: str
    phone: str

@customer_router.post("/mongo")
def create_customer_mongo(customer: CustomerData):
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client["online_customers_db"]
        collection = db["online_customers"]
        customer_id = collection.insert_one(customer.dict()).inserted_id
        return {"id": str(customer_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@customer_router.post("/postgres")
def create_customer_postgres(customer: CustomerData):
    try:
        engine = sqlalchemy.create_engine('postgresql+asyncpg://username:password@localhost/mydatabase')
        with engine.connect() as connection:
            connection.execute("INSERT INTO offline_customers (name, email, phone) VALUES (:name, :email, :phone)", customer.dict())
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@customer_router.get("/combined")
def get_combined_customers():
    try:
        engine = sqlalchemy.create_engine('postgresql+asyncpg://username:password@localhost/new_database')
        with engine.connect() as connection:
            result = pd.read_sql("SELECT * FROM combined_customers", connection)
        return result.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))