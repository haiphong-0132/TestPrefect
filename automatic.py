from prefect import flow
from prefect.runner.storage import GitRepository
from prefect.blocks.system import Secret
from prefect.client.schemas.schedules import CronSchedule

REPO = "https://github.com/haiphong-0132/TestPrefect.git"

PARAMETERS = Secret.load('parameters').get()

if __name__ == "__main__":
    flow.from_source(
        source=GitRepository(
            url=REPO,
            credentials={
                'access_token': Secret.load('github-access-token'),
            },
        ),
        entrypoint="etl_pipeline.py:etl_pipeline",
    ).deploy(
        name="my-etl-pipeline",
        schedules=[
            CronSchedule(cron="*/2 * * * *")
        ],

        parameters= {"parameters": PARAMETERS},
        work_pool_name="etl-pipeline-tool",
    )
