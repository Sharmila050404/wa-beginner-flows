from prefect import flow, task

@task
def auto_step():
    return "Auto step done"

@task
def manual_step(approved: bool):
    if not approved:
        raise Exception("Not approved")

@flow
def approval_workflow(approved: bool = False):
    auto_step()
    manual_step(approved)
