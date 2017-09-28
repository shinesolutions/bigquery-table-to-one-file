# bigquery-table-to-one-file
Using Cloud Dataflow, this Java app reads a table in BigQuery, and turns it into one file in GCS. Why? Because BigQuery only support unsharded exports under 1GB.

Program uses the default credentials set to the environment variable `GOOGLE_APPLICATION_CREDENTIALS`. See all about that here: https://developers.google.com/identity/protocols/application-default-credentials

In the code, change the table name and bucket details etc. to suit your needs. You will also just need to create the GCS bucket(s) yourself. I wasn't bothered making them cli parameters.

To run:

`--project=<your_project_id>
--runner=DataflowRunner
--jobName=bigquery-table-to-one-file
--maxNumWorkers=50
--zone=australia-southeast1-a
--stagingLocation=gs://<your_bucket>/jars
--tempLocation=gs://<your_bucket>/tmp`

It should look like this when it's running:
