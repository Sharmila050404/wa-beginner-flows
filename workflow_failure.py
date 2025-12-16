from prefect import flow, task

@task
def failing_task():
    raise Exception("Simulated failure")

@flow
def failure_workflow():
    failing_task()
