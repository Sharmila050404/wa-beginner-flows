from prefect import flow, task
import time

@task
def wait_task():
    time.sleep(10)

@flow
def wait_workflow():
    wait_task()
