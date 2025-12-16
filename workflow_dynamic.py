from prefect import flow, task

@task
def process_item(item):
    return f"Processed {item}"

@flow
def dynamic_workflow():
    items = ["A", "B", "C", "D"]
    for item in items:
        process_item(item)
