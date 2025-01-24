from prefect import flow
from pathlib import Path


@flow(log_prints=True)
def my_flow(name: str = "World"):
    print(f"Hello {name}!")
    print(str(Path(__file__).parent))  # dynamic path


if __name__ == "__main__":
    my_flow.from_source(
        source=str(Path(__file__).parent),  # code stored in local directory
        entrypoint="token_pipeline.py:validation_flow",
    ).deploy(
        name="local-rdstation-token-validation-pipeline",
        work_pool_name="digitalocean-pool",
        cron='00 03 * * *'
    )
