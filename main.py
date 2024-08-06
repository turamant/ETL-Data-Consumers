from fastapi import FastAPI

from my_dagster.dagster_project import etl_job, DagsterInstance


app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.post("/etl/run")
def run_etl():
    instance = DagsterInstance.get()
    result = etl_job.execute_in_process(instance=instance)
    if result.success:
        return {"status": "ETL succeeded"}
    return {"status": "ETL failed"}
