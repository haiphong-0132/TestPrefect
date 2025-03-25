from prefect import flow

if __name__ == '__main__':
    flow.from_source(
        source='./',
        entrypoint='etl_pipeline.py:etl_pipeline',
    ).deploy(
        name='my-etl-pipeline',
        parameters={},
        work_pool_name='my-work-pool',
        cron='* * * * *'
    )