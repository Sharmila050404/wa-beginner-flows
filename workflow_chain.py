from prefect import flow, task

@task
def job_one():
    return "Job One Complete"

@task
def job_two():
    return "Job Two Complete"

@task
def job_three():
    return "Job Three Complete"

@flow
def flow_one():
    job_one()

@flow
def flow_two():
    job_two()

@flow
def flow_three():
    job_three()
