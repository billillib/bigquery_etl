from google.cloud import bigquery
import logging

# Instanstiate a client
client = bigquery.Client()

logging.basicConfig(filename='test.log', level=logging.INFO)

# Destination bucket - to be assigned via YAML
bucket_name = 'billw_sample_bucket'

# Set Source Dataset
project = 'bigquery-public-data'
dataset_id = 'census_bureau_usa'
dataset_ref = client.dataset(dataset_id, project=project)

# Set extract job config
extract_job_config = bigquery.ExtractJobConfig()
extract_job_config.compression = 'GZIP'

# Loop over tables in the dataset and export to Google Storage
tables = list(client.list_tables(dataset_ref))

for table in tables:
    table_name = table.table_id
    destination_uri = 'gs://{}/{}'.format(bucket_name, table_name + '.csv')
    table_ref = dataset_ref.table(table_name)
    # API Request to extract
    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        job_config=extract_job_config,
        # Location must match that of the source table.
        location='US')
    extract_job.result()  # Waits for job to complete.
    # Log result
    logging.info('Exported {}:{}.{} to {}'.format(project, dataset_id,
                                                  table_name, destination_uri))
