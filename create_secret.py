import os
from dotenv import load_dotenv
from prefect.blocks.system import Secret

load_dotenv()

Secret(value=os.getenv('DB_DRIVER')).save(name='db-driver', overwrite=True)
Secret(value=os.getenv('DB_SERVER')).save(name='db-server', overwrite=True)
Secret(value=os.getenv('DB_NAME')).save(name='db-name', overwrite=True)
Secret(value=os.getenv('DB_USER')).save(name='db-user', overwrite=True)
Secret(value=os.getenv('DB_PASSWORD')).save(name='db-password', overwrite=True)
Secret(value=os.getenv('DB_TRUST_SERVER_CERTIFICATE')).save(name='db-trust-server-certificate', overwrite=True)

with open('credentials.json') as f:
    credentials = f.read()

Secret(value=credentials).save(name='google-credentials', overwrite=True)