from prefect import flow, task

@task
def extract():
    return "Data Extracted"

@task
def transform(data):
    return f"{data} → Transformed"

@task
def load(data):
    return f"{data}"  # logs will NOT appear in Prefect Cloud

@flow
def etl_flow(job_name: str = "Default Job"):
    raw = extract()
    processed = transform(raw)
    load(f"{job_name}: {processed}")
