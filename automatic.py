from prefect import flow

REPO = "https://github.com/haiphong-0132/TestPrefect.git"

if __name__ == "__main__":
    flow.from_source(
        source=REPO,
        entrypoint="etl_pipeline.py:etl_pipeline",
    ).deploy(
        name="my-etl-pipeline",
        job_variables={
            "env": {
                "EXTRA_PIP_PACKAGES": "-r ./requirements.txt"
            }
        },
        parameters={},
        work_pool_name="my-work-pool",
        cron="0 0 * * *"  # Run daily at midnight
    )
