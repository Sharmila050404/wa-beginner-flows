from prefect import flow, task
    
@task
def failing_task():
   raise Exception("Simulated failure")
    
@task(retries=3, retry_delay_seconds=5)
def retry_task():
    raise Exception("Retrying")
    
@flow
def failure_workflow():
    retry_task()

