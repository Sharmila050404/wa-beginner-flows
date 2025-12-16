from prefect import flow, task
    
@task(retries=3, retry_delay_seconds=5)
def retry_task():
    raise Exception("Retrying")
    
@flow
def failure_workflow():
    failing_task()

