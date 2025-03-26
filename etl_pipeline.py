# import json
# import os
# import pyodbc
# import pandas as pd
# import gspread
# from google.oauth2.service_account import Credentials
# from prefect import flow, task
# from dotenv import load_dotenv
# from prefect.blocks.system import Secret

# load_dotenv()

# conn_str = (
#     f"DRIVER={Secret.load('db-driver').get()};"
#     f"SERVER={Secret.load('db-server').get()};"
#     f"DATABASE={Secret.load('db-name').get()};"
#     f"UID={Secret.load('db-user').get()};"
#     f"PWD={Secret.load('db-password').get()};"
#     f"TrustServerCertificate={Secret.load('db-trust-server-certificate').get()};"
# )

# @task(retries=3, retry_delay_seconds=10)
# def extract_from_google_sheet(sheet_url: str):
#     scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

#     creds_json = Secret.load('google-credentials').get()
#     creds_dict = json.loads(json.dumps(creds_json))
#     creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
#     client = gspread.authorize(creds)
#     sheet = client.open_by_url(sheet_url)
#     worksheet = sheet.get_worksheet(0)
#     records = worksheet.get_all_records()
#     return pd.DataFrame(records)

# @task
# def transform_data(df: pd.DataFrame, table_name: str):
#     df.dropna(inplace=True)
#     df.drop_duplicates(inplace=True)
#     for col in df.columns:
#         if pd.api.types.is_string_dtype(df[col]):
#             try:
#                 df[col] = pd.to_numeric(df[col], errors='ignore')
#             except Exception as e:
#                 print(f"Failed to convert column '{col}': {e}")
            
#     return df, table_name

# @task
# def load_to_sql_server(df: pd.DataFrame, table_name: str):
#     with pyodbc.connect(conn_str) as conn:
        
#         cursor = conn.cursor()

#         columns = ', '.join([f"[{col}] NVARCHAR(255)" for col in df.columns])
#         cursor.execute(
#             f"""
#             IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
#             CREATE TABLE [{table_name}] (
#                 {columns}
#             )
#             """
#         )
        
#         # Batch insert data
#         placeholders = ', '.join(['?' for _ in df.columns])
#         query = f"INSERT INTO [{table_name}] ({', '.join([f'[{col}]' for col in df.columns])}) VALUES ({placeholders})"
#         data = [tuple(row) for row in df.itertuples(index=False)]
#         cursor.fast_executemany = True  # Tăng tốc độ khi chèn batch
#         cursor.executemany(query, data)
#         conn.commit()

# @flow
# def etl_pipeline():
#     sheet_url = 'https://docs.google.com/spreadsheets/d/1ZGbOTAP2W2MIfo5HWvMR40RF6oWSUfR4Va9E_RvczWk/edit?gid=1647286845#gid=1647286845'
#     df = extract_from_google_sheet(sheet_url)
#     df, table_name = transform_data(df, 'Customers')
#     load_to_sql_server(df, table_name)

# if __name__ == '__main__':
#     etl_pipeline()

import os
import re
import json
import pyodbc
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(
    cache_key_fn=task_input_hash, 
    cache_expiration=timedelta(hours=1),
    retries=3, 
    retry_delay_seconds=10
)
def extract_sheet_key(sheet_url: str) -> str:
    """
    Extract the Google Sheets document key from various URL formats
    """
    # Different possible URL patterns
    patterns = [
        r'/spreadsheets/d/([a-zA-Z0-9-_]+)',  # Standard format
        r'https://docs.google.com/spreadsheets/d/([a-zA-Z0-9-_]+)',
        r'docs.google.com/spreadsheets/d/([a-zA-Z0-9-_]+)',
        r'spreadsheets.google.com/spreadsheets/d/([a-zA-Z0-9-_]+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, sheet_url)
        if match:
            return match.group(1)
    
    raise ValueError(f"Could not extract sheet key from URL: {sheet_url}")

@task(
    retries=3, 
    retry_delay_seconds=10, 
    cache_key_fn=task_input_hash, 
    cache_expiration=timedelta(hours=1)
)
def extract_from_google_sheet(sheet_url: str):
    try:
        # Load credentials from Prefect Secret
        creds_json = Secret.load('google-credentials').get()
        
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets', 
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
        
        client = gspread.authorize(creds)
        
        # Extract sheet key and open by key
        sheet_key = extract_sheet_key(sheet_url)
        sheet = client.open_by_key(sheet_key)
        
        # Determine which worksheet to use
        worksheet_match = re.search(r'gid=(\d+)', sheet_url)
        if worksheet_match:
            worksheet_id = int(worksheet_match.group(1))
            worksheet = sheet.worksheet(sheet.worksheet_by_title(sheet.worksheet(worksheet_id).title).title)
        else:
            worksheet = sheet.get_worksheet(0)  # Default to first worksheet
        
        records = worksheet.get_all_records()
        df = pd.DataFrame(records)
        
        print(f"Extracted {len(df)} rows from Google Sheet")
        return df
    
    except Exception as e:
        print(f"Sheet extraction error: {e}")
        raise

@task
def transform_data(df: pd.DataFrame):
    """
    Perform data transformation
    """
    if df.empty:
        print("Warning: Empty DataFrame received")
        return df
    
    # Remove duplicate rows
    df.drop_duplicates(inplace=True)
    
    # Remove rows with all null values
    df.dropna(how='all', inplace=True)
    
    # Convert columns to appropriate types
    for col in df.columns:
        # Try to convert to numeric, ignore errors
        try:
            df[col] = pd.to_numeric(df[col], errors='ignore')
        except Exception as e:
            print(f"Could not convert column {col}: {e}")
    
    print(f"Transformed DataFrame. Rows: {len(df)}, Columns: {list(df.columns)}")
    return df

@task
def load_to_sql_server(df: pd.DataFrame, table_name: str):
    """
    Load transformed data to SQL Server
    """
    # Retrieve connection details from Prefect Secrets
    conn_str = (
        f"DRIVER={Secret.load('db-driver').get()};"
        f"SERVER={Secret.load('db-server').get()};"
        f"DATABASE={Secret.load('db-name').get()};"
        f"UID={Secret.load('db-user').get()};"
        f"PWD={Secret.load('db-password').get()};"
        f"TrustServerCertificate={Secret.load('db-trust-server-certificate').get()};"
    )

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            
            # Create table if not exists with dynamic columns
            columns = ', '.join([
                f"[{col}] {get_sql_type(df[col])}" 
                for col in df.columns
            ])
            
            cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
                CREATE TABLE [{table_name}] ({columns})
            """)
            
            # Prepare insert query
            placeholders = ', '.join(['?' for _ in df.columns])
            query = f"INSERT INTO [{table_name}] ({', '.join(df.columns)}) VALUES ({placeholders})"
            
            # Prepare data for insertion
            data = [
                tuple(
                    None if pd.isna(x) else str(x) 
                    for x in row
                ) 
                for row in df.itertuples(index=False)
            ]
            
            # Batch insert
            cursor.fast_executemany = True
            cursor.executemany(query, data)
            conn.commit()
            
            print(f"Loaded {len(data)} rows to {table_name}")
    
    except Exception as e:
        print(f"Error loading to SQL Server: {e}")
        raise

def get_sql_type(series):
    """
    Determine appropriate SQL data type based on pandas series
    """
    if pd.api.types.is_integer_dtype(series):
        return 'INT'
    elif pd.api.types.is_float_dtype(series):
        return 'FLOAT'
    elif pd.api.types.is_datetime64_any_dtype(series):
        return 'DATETIME'
    else:
        return 'NVARCHAR(MAX)'

@flow(
    log_prints=True, 
    description="ETL Pipeline for Google Sheets to SQL Server",
    flow_run_name="etl-{sheet_url}"
)
def etl_pipeline(
    sheet_url: str = 'https://docs.google.com/spreadsheets/d/1ZGbOTAP2W2MIfo5HWvMR40RF6oWSUfR4Va9E_RvczWk/edit?gid=1647286845#gid=1647286845', 
    table_name: str = 'Customers'
):
    """
    Main ETL pipeline flow
    
    Args:
        sheet_url (str): URL of the Google Sheet to extract data from
        table_name (str): Name of the table to load data into
    """
    # Extract data from Google Sheet
    df = extract_from_google_sheet(sheet_url)
    
    # Transform data
    df_cleaned = transform_data(df)
    
    # Load to SQL Server
    load_to_sql_server(df_cleaned, table_name)

if __name__ == '__main__':
    etl_pipeline()