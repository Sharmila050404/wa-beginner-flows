from prefect import flow, task

@task
def first_step():
    return "First step finished"

@task
def second_step(data):
    return f"{data} -> Second step finished"

@task
def third_step(data):
    return f"{data} -> Third step finished"

@flow
def basic_workflow():
    step1 = first_step()
    step2 = second_step(step1)
    third_step(step2)
