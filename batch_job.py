from prefect import flow, task

@task
def extract():
return "Data Extracted"

@task
def transform(data):
return f"{data} → Transformed"

@task
def load(data):
print(f"Loading: {data}")

@flow
def etl_flow():
raw = extract()
processed = transform(raw)
load(processed)

if __name__ == "__main__":
etl_flow()
