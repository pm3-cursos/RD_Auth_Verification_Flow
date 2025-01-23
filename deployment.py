from prefect import flow

# Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO="https://github.com/pm3-cursos/RD_Auth_Verification_Flow.git"

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="token_pipeline.py:validation_flow", # Specific flow to run
    ).deploy(
        name="Validacao RD Station",
        work_pool_name="principal-work-pool",
        cron="45 15 * * *",
        timezone="America/Sao_Paulo"  # Run every hour
    )