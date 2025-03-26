import json
import os
import pyodbc
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from prefect import flow, task
from dotenv import load_dotenv
from prefect.blocks.system import Secret

load_dotenv()

conn_str = (
    f"DRIVER={Secret.load('db-driver').get()};"
    f"SERVER={Secret.load('db-server').get()};"
    f"DATABASE={Secret.load('db-name').get()};"
    f"UID={Secret.load('db-user').get()};"
    f"PWD={Secret.load('db-password').get()};"
    f"TrustServerCertificate={Secret.load('db-trust-server-certificate').get()};"
)

@task
def extract_from_google_sheet(sheet_url: str):
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

    creds_json = Secret.load('google-credentials').get()
    creds_dict = json.loads(json.dumps(creds_json))
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(sheet_url)
    worksheet = sheet.get_worksheet(0)
    records = worksheet.get_all_records()
    return pd.DataFrame(records)

@task
def transform_data(df: pd.DataFrame, table_name: str):
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            try:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except Exception as e:
                print(f"Failed to convert column '{col}': {e}")
            
    return df, table_name

@task
def load_to_sql_server(df: pd.DataFrame, table_name: str):
    with pyodbc.connect(conn_str) as conn:
        
        cursor = conn.cursor()

        columns = ', '.join([f"[{col}] NVARCHAR(255)" for col in df.columns])
        cursor.execute(
            f"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
            CREATE TABLE [{table_name}] (
                {columns}
            )
            """
        )
        
        # Batch insert data
        placeholders = ', '.join(['?' for _ in df.columns])
        query = f"INSERT INTO [{table_name}] ({', '.join([f'[{col}]' for col in df.columns])}) VALUES ({placeholders})"
        data = [tuple(row) for row in df.itertuples(index=False)]
        cursor.fast_executemany = True  # Tăng tốc độ khi chèn batch
        cursor.executemany(query, data)
        conn.commit()

@flow
def etl_pipeline():
    sheet_url = 'https://docs.google.com/spreadsheets/d/1ZGbOTAP2W2MIfo5HWvMR40RF6oWSUfR4Va9E_RvczWk/edit?gid=1647286845#gid=1647286845'
    df = extract_from_google_sheet(sheet_url)
    df, table_name = transform_data(df, 'Customers')
    load_to_sql_server(df, table_name)

if __name__ == '__main__':
    etl_pipeline()