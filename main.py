from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.post("/etl/run")
def run_etl():
    # Здесь будет вызываться ETL процесс
    return {"status": "ETL запуск"}
