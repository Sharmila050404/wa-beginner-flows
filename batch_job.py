from prefect import flow, task
from prefect import get_run_logger
@task
def extract():
    logger = get_run_logger()
    logger.info("Starting extract task")
    return "Data Extracted"

@task
def transform(data):
   return f"{data} → Transformed"

"""@task(retries=3, retry_delay_seconds=30)
def transform(data):
    import random
    if random.choice([True, False]):
        raise Exception("Random simulated failure")
    return f"{data} → Transformed"
"""
@task
def load(data):
    return f"{data}"  # logs will NOT appear in Prefect Cloud

@flow
def etl_flow(job_name: str = "Default Job"):
    raw = extract()
    processed = transform(raw)
    load(f"{job_name}: {processed}")
"""
@flow
def etl_flow(file_name: str = "default_file.csv"):
    raw = extract()
    processed = transform(raw)
    load(processed, file_name)

@task
def load(data, file_name):
    print(f"Loading: {data} into {file_name}")
"""
