import uuid
from google.cloud import bigquery

''' Path of the json with the account information '''
service_account_file_path = 'C:\Service accounts\google_service_account.json'

''' Function to run any SQL quey on Google BigQuery and return the data '''
def get_table_from_sql_qry(sql_query):
    client = bigquery.Client.from_service_account_json(service_account_file_path)
    job_name = str(uuid.uuid4())
    query_job = client.run_async_query(job_name, sql_query)
    query_job.begin()
    query_job.result()    
    destination_table = query_job.destination
    destination_table.reload()    
    return destination_table.fetch_data()