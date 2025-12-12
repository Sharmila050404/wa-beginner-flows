from prefect import flow, task

@task
def step1():
    print("Step 1 complete")

@task
def step2():
    print("Step 2 complete")

@task
def step3():
    print("Step 3 complete")

@flow
def sequence_flow():
    s1 = step1()
    s2 = step2(wait_for=[s1])
    step3(wait_for=[s2])
