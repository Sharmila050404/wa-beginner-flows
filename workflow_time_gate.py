from prefect import flow, task
from datetime import datetime

@task
def time_check():
    current_hour = datetime.now().hour
    return 9 <= current_hour <= 18

@task
def gated_task():
    return "Ran during allowed window"

@flow
def time_gated_workflow():
    allowed = time_check()
    if allowed:
        gated_task()
