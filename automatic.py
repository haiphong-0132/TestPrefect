from prefect import flow
from prefect.runner.storage import GitRepository
from prefect.blocks.system import Secret
from prefect.client.schemas.schedules import CronSchedule

REPO = "https://github.com/haiphong-0132/TestPrefect.git"

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
        # Parameter has 2: sheet_url, table_name
        # Multiple parameters are separated by commas
        parameters={
            'sheet_url': 'https://docs.google.com/spreadsheets/d/15rOu4yxl_mD94y6fKyxeBVHNi4SgOrjI-7q2lxpjyQo/edit?gid=1513155777#gid=1513155777',
            'table_name': 'Employees'
        },
        work_pool_name="etl-pipeline-tool",
    )
