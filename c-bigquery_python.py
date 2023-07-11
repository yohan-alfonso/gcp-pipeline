from google.cloud import bigquery

input_file = 'output_salarios-00000-of-00001.csv'

# Set up your Google Cloud project ID and BigQuery dataset and table names
project_id = "gcp-pipeline-391818"
dataset_id = "gcp-pipeline-391818.RRHH"
table_id = "gcp-pipeline-391818.RRHH.salarios"

# Set up the BigQuery client
client = bigquery.Client(project=project_id)

schema = [
    bigquery.SchemaField("initial_sal", "FLOAT"),
    bigquery.SchemaField("final_sal", "FLOAT"),
    bigquery.SchemaField("avg_sal", "FLOAT"),
    bigquery.SchemaField("job_type", "STRING"),
]

# Set up the job configuration
job_config = bigquery.LoadJobConfig(
    #autodetect=True,  # Automatically detect schema from the data
    schema=schema,
    skip_leading_rows=0,  # Skip the header row
    source_format=bigquery.SourceFormat.CSV,
    field_delimiter=","
)

# Set the path to your CSV file
data_file = input_file

# Start the BigQuery load job
with open(data_file, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

# Wait for the job to complete
job.result()

# Check the status of the job
if job.state == "DONE":
    print("Data loaded into BigQuery table.")
else:
    print("Error loading data into BigQuery table:", job.errors)
