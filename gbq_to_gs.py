from google.cloud import bigquery
import logging
import yaml

# Read yaml config
with open('test.yaml', 'r') as f:
    config = yaml.load(f)

# Set logging parameters
logging.basicConfig(filename='test.log', level=logging.INFO)

# Instanstiate a client
client = bigquery.Client()

# Destination properties
bucket_name = config["target"]["bucket_name"]

# Source properties
project = config["source"]["project"]
dataset_id = config["source"]["dataset_id"]
dataset_ref = client.dataset(dataset_id, project=project)
file_extention = config["target"]["file_extension"]

# Set extract job config
extract_job_config = bigquery.ExtractJobConfig()
extract_job_config.compression = 'GZIP'

# Generate a list of tables in the dataset
tables = list(client.list_tables(dataset_ref))

# Loop over tables in the dataset and export to Google Storage
for table in tables:
    table_name = table.table_id
    destination_uri = 'gs://{}/{}'.format(bucket_name, table_name + '.' +
                                          file_extention)
    table_ref = dataset_ref.table(table_name)
    table_details = client.get_table(table_ref)
    # API Request to extract
    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        job_config=extract_job_config,
        # Location must match that of the source table.
        location='US')
    extract_job.result()  # Waits for job to complete.
    # Log result
    logging.info('Exported {}:{}.{} to {}. {} rows'.format(project, dataset_id,
                                                           table_name,
                                                           destination_uri,
                                                           table_details.num_rows))
