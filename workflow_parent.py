from prefect import flow
from workflow_basic import basic_workflow
from workflow_parallel import parallel_workflow

@flow
def parent_workflow():
    basic_workflow()
    parallel_workflow()
