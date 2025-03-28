import os
from dotenv import load_dotenv
from prefect.blocks.system import Secret

load_dotenv()

with open('credentials.json') as f:
    credentials = f.read()


with open('parameters.json') as f:
    parameters = f.read()

Secret(value=os.getenv('DB_DRIVER')).save(name='db-driver', overwrite=True)
Secret(value=os.getenv('DB_SERVER')).save(name='db-server', overwrite=True)
Secret(value=os.getenv('DB_NAME')).save(name='db-name', overwrite=True)
Secret(value=os.getenv('DB_USER')).save(name='db-user', overwrite=True)
Secret(value=os.getenv('DB_PASSWORD')).save(name='db-password', overwrite=True)
Secret(value=os.getenv('DB_TRUST_SERVER_CERTIFICATE')).save(name='db-trust-server-certificate', overwrite=True)
Secret(value=os.getenv('GITHUB_ACCESS_TOKEN')).save(name='github-access-token', overwrite=True)
Secret(value=credentials).save(name='google-credentials', overwrite=True)
Secret(value=parameters).save(name='parameters', overwrite=True)