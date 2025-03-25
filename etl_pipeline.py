import os
import pyodbc
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from prefect import flow, task
from dotenv import load_dotenv

load_dotenv()

conn_str = (
    f"DRIVER={os.getenv('DB_DRIVER')};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')};"
    f"TrustServerCertificate={os.getenv('DB_TRUST_SERVER_CERTIFICATE')};"
)

@task
def extract_from_google_sheet(sheet_url: str):
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
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
    sheet_url = 'https://docs.google.com/spreadsheets/d/1a84Lw9MdZDXHBHtoN_bkjLUW6EhnjasHzF07SD1XLSA/edit#gid=1460457617'
    df = extract_from_google_sheet(sheet_url)
    df, table_name = transform_data(df, 'Products')
    load_to_sql_server(df, table_name)

if __name__ == '__main__':
    etl_pipeline()
