# bigquery-table-to-one-file
Using Cloud Dataflow, this Java app reads a table in BigQuery, and turns it into one file in GCS (BigQuery only support sharded exports over 1GB).
It Uses the default credentials set to the environment variable `GOOGLE_APPLICATION_CREDENTIALS`
In the code, change the table name and bucket details etc. to suit your needs. You will need to create the GCS buckets yourself.

To run:

`--project=<your_project_id>
--runner=DataflowRunner
--jobName=bigquery-table-to-one-file
--maxNumWorkers=50
--zone=australia-southeast1-a
--stagingLocation=gs://<your_bucket>/jars
--tempLocation=gs://<your_bucket>/tmp`
