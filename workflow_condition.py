from prefect import flow, task

@task
def always_run():
    return "Always runs"

@task
def optional_run():
    return "Runs only if enabled"

@flow
def conditional_workflow(run_optional: bool = False):
    always_run()
    if run_optional:
        optional_run()
