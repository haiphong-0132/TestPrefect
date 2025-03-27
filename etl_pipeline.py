# # prefect worker start --pool etl-pipeline-tool
# import json
# import pyodbc
# import pandas as pd
# import gspread
# from google.oauth2.service_account import Credentials
# from prefect import flow, task
# from prefect.blocks.system import Secret
# from prefect.tasks import task_input_hash
# from datetime import timedelta

# conn_str = (
#     f"DRIVER={Secret.load('db-driver').get()};"
#     f"SERVER={Secret.load('db-server').get()};"
#     f"DATABASE={Secret.load('db-name').get()};"
#     f"UID={Secret.load('db-user').get()};"
#     f"PWD={Secret.load('db-password').get()};"
#     f"TrustServerCertificate={Secret.load('db-trust-server-certificate').get()};"
# )

# @task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
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

#         def get_column_type(series):
#             if pd.api.types.is_numeric_dtype(series):
#                 if pd.api.types.is_integer_dtype(series):
#                     return 'INT'
#                 else:
#                     return 'FLOAT'
#             else:
#                 max_length = series.astype(str).str.len().max()

#                 max_length = min(max_length, 4000) 
#                 return f'NVARCHAR({max_length})'

#         column_defs = []
#         for col in df.columns:
#             col_type = get_column_type(df[col])
#             column_defs.append(f"[{col}] {col_type}")
        
#         columns_str = ', '.join(column_defs)

#         cursor.execute(f"IF OBJECT_ID('{table_name}') IS NOT NULL DROP TABLE [{table_name}]")
        
#         cursor.execute(
#             f"""
#             CREATE TABLE [{table_name}] (
#                 {columns_str}
#             )
#             """
#         )
        
#         placeholders = ', '.join(['?' for _ in df.columns])
#         query = f"INSERT INTO [{table_name}] ({', '.join([f'[{col}]' for col in df.columns])}) VALUES ({placeholders})"
        
#         def convert_row(row):
#             return tuple(
#                 str(val)[:4000] if isinstance(val, str) else 
#                 val for val in row
#             )
        
#         data = [convert_row(row) for row in df.itertuples(index=False)]
        
#         cursor.fast_executemany = True
#         cursor.executemany(query, data)
#         conn.commit()

#         return f'Loaded {table_name} with {len(data)} rows'

# @task
# def process_sheet(sheet_info: dict):
#     sheet_url, table_name = sheet_info['sheet_url'], sheet_info['table_name']
#     df = extract_from_google_sheet(sheet_url)
#     df, table_name = transform_data(df, table_name)
#     result = load_to_sql_server(df, table_name)
#     return result

# @flow(log_prints=True)
# def etl_pipeline(sheets: list[dict[str, str]]):
#     results = process_sheet.map(sheets)
#     return results

# if __name__ == "__main__":
#     etl_pipeline(
#         [
#         {
#             "sheet_url": "https://docs.google.com/spreadsheets/d/1TVk7_vQbl__q5a4sAgBf6_eKspeut7q_ch55VVAvsW4/edit?gid=152346427#gid=152346427",
#             "table_name": "categories"
#         }, 

#         {
#             "sheet_url": "https://docs.google.com/spreadsheets/d/1TSWWA0GGVi3BNK9R38jK9RFQHZf7FCR-x6w0F-PY-8k/edit?gid=1925009959#gid=1925009959",
#             "table_name": "cate_prd_supp"
#         },
#         ]
#     )

import json
import pyodbc
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.tasks import task_input_hash
from datetime import timedelta

conn_str = (
    f"DRIVER={Secret.load('db-driver').get()};"
    f"SERVER={Secret.load('db-server').get()};"
    f"DATABASE={Secret.load('db-name').get()};"
    f"UID={Secret.load('db-user').get()};"
    f"PWD={Secret.load('db-password').get()};"
    f"TrustServerCertificate={Secret.load('db-trust-server-certificate').get()};"
)

@task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
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
def extract_all_dataframes(parameters: list[dict]):
    dataframes = {}
    for param in parameters:
        df = extract_from_google_sheet(param['sheet_url'])
        dataframes[param['table_name']] = df
    return dataframes

@task
def merge_tables(dataframes: dict):
    # Merge steps as specified in the requirements
    merge_order = [
        ('categories', 'products', 'categoryID'),
        ('cate_prd', 'suppliers', 'supplierID'),
        ('cate_prd_supp', 'order_details', 'productID'),
        ('cate_prd_supp_order_details', 'orders', 'orderID'),
        ('cate_prd_supp_order_details_ord', 'customers', 'customerID'),
        ('cate_prd_supp_order_details_ord_cus', 'employees', 'employeeID'),
        ('cate_prd_supp_order_details_ord_cus_empl', 'employee_territories', 'employeeID'),
        ('cate_prd_supp_order_details_ord_cus_empl_emplt', 'territories', 'territoryID'),
        ('cate_prd_supp_order_details_ord_cus_empl_emplt_teri', 'regions', 'regionID'),
        
    ]

    df_merged = dataframes['categories']

    for prev_name, next_name, merge_key in merge_order:
        # Merge the previous merged dataframe with the next table
        df_merged = pd.merge(df_merged, dataframes[next_name], on=merge_key, how='inner')

    return df_merged

@task
def load_to_sql_server(df: pd.DataFrame, table_name: str = 'final_merged_table'):
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()

        def get_column_type(series):
            if pd.api.types.is_numeric_dtype(series):
                if pd.api.types.is_integer_dtype(series):
                    return 'INT'
                else:
                    return 'FLOAT'
            else:
                max_length = series.astype(str).str.len().max()
                max_length = min(max_length, 4000) 
                return f'NVARCHAR({max_length})'

        column_defs = []
        for col in df.columns:
            col_type = get_column_type(df[col])
            column_defs.append(f"[{col}] {col_type}")
        
        columns_str = ', '.join(column_defs)

        # Drop table if it exists
        cursor.execute(f"IF OBJECT_ID('{table_name}') IS NOT NULL DROP TABLE [{table_name}]")
        
        # Create table with dynamic columns
        cursor.execute(
            f"""
            CREATE TABLE [{table_name}] (
                {columns_str}
            )
            """
        )
        
        # Prepare insert query
        placeholders = ', '.join(['?' for _ in df.columns])
        query = f"INSERT INTO [{table_name}] ({', '.join([f'[{col}]' for col in df.columns])}) VALUES ({placeholders})"
        
        # Convert rows to handle string truncation
        def convert_row(row):
            return tuple(
                str(val)[:4000] if isinstance(val, str) else 
                val for val in row
            )
        
        data = [convert_row(row) for row in df.itertuples(index=False)]
        
        # Insert data
        cursor.fast_executemany = True
        cursor.executemany(query, data)
        conn.commit()

        return f'Loaded {table_name} with {len(data)} rows'

@flow(log_prints=True)
def etl_merge_pipeline(parameters: list[dict]):
    # Extract all dataframes
    dataframes = extract_all_dataframes(parameters)
    
    # Merge tables
    merged_df = merge_tables(dataframes)
    
    # Load merged table to SQL Server
    result = load_to_sql_server(merged_df)
    
    return result

if __name__ == "__main__":
    # Load parameters from the same parameters.json file
    with open('parameters.json', 'r') as f:
        parameters = json.load(f)
    
    etl_merge_pipeline(parameters)