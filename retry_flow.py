from prefect import flow, task
from random import randint

@task(retries=3, retry_delay_seconds=2)
def unstable_task():
    n = randint(1, 10)
    if n <= 7:
        raise Exception("Random failure!")
        print("Success on attempt")

@flow
def retry_example():
    unstable_task()
