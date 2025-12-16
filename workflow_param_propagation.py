from prefect import flow, task

@task
def generate_batch_id():
    return "BATCH_001"

@flow
def producer_flow():
    return generate_batch_id()

@flow
def consumer_flow(batch_id: str):
    return batch_id

@flow
def orchestrator():
    batch_id = producer_flow()
    consumer_flow(batch_id)
