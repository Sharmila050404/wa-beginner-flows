from prefect import flow, task

@task
def risky_task():
    raise Exception("Failure allowed")

@task
def continue_task():
    return "Continued execution"

@flow
def soft_fail_workflow():
    try:
        risky_task()
    except:
        pass
    continue_task()
