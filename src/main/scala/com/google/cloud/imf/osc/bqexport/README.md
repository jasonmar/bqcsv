# BQExport

This utility exports a BigQuery table to GCS
as gzipped csv in partitions of ~50M rows.

Output files are generated based on the table name and
have a suffix indicating stream id and partition id for that stream.

For a `--destUri` of `gs://bucket/prefix`, the first
resulting filename will be `gs://bucket/prefix/tablename-0-0.csv.gz`


## Help Text

```
BQExport - Google Cloud Open Systems Connector

*  Exports BigQuery table to GCS as ORC

Usage: com.google.cloud.imf.BQExport [options]

  --help                   prints this usage text
  --billingProject <value>
                           Billing project ID
  --projectId <value>      Project ID
  --dataset <value>        Source Dataset
  --table <value>          Source Table
  --destUri <value>        Destination URI
  --paralellism <value>    Number of Threads
```

example

```sh
java -Xms1g -Xmx1g \
  -cp "open-systems-connector_2.13-0.6.4-SNAPSHOT.jar:open-systems-connector-assembly-0.6.4-SNAPSHOT-deps.jar" \
  com.google.cloud.imf.BQExport \
  --billingProject project \
  --projectId project \
  --dataset dataset \
  --table table \
  --paralellism 2 \
  --destUri gs://bucket/prefix
```
