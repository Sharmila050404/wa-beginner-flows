from prefect import flow, task

@task
def extract_data():
    return "Extracted"

@task
def validate_data(data):
    return "Validated"

@task
def enrich_data(data):
    return "Enriched"

@task
def load_data(v, e):
    return "Loaded"

@flow
def parallel_workflow():
    data = extract_data()
    v = validate_data(data)
    e = enrich_data(data)
    load_data(v, e)
