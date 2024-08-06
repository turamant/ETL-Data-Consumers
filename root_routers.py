from fastapi import APIRouter
from my_dagster.dagster_project import etl_job, DagsterInstance

root_router = APIRouter()

@root_router.get("/")
def read_root():
    return {"Hello": "World"}

@root_router.post("/etl/run")
def run_etl():
    instance = DagsterInstance.ephemeral()
    result = etl_job.execute_in_process(instance=instance)
    if result.success:
        return {"status": "ETL succeeded"}
    return {"status": "ETL failed"}
