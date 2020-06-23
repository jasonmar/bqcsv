# BQExport

This utility exports a BigQuery table to GCS
as ORC file format or Sequence file format as .seq or gzipped csv in partitions of ~50M rows.

Output files are generated based on the table name and
have a suffix indicating stream id and partition id for that stream.

For a `--destUri` of `gs://bucket/prefix`, the first
resulting filename will be `gs://bucket/prefix/tablename-0-0.csv.gz` or `gs://bucket/prefix/tablename-0-0.seq`


## Help Text

```
BQExport - Google Cloud Open Systems Connector

*  Exports BigQuery table to GCS as ORC or SEQ format

Usage: com.google.cloud.imf.BQExport [options]

  --help                   prints this usage text
  --billingProject <value>
                           Billing project ID
  --projectId <value>      Project ID
  --dataset <value>        Source Dataset
  --table <value>          Source Table
  --destUri <value>        Destination URI
  --paralellism <value>    Number of Threads
  --outputFileType <value> Sequence(SEQ) or ORC File types. If nothing specified, it uses ORC File as output. 
```

example

```sh
java -Xms1g -Xmx1g \
  -cp "open-systems-connector_2.13-0.6.7-SNAPSHOT.jar:open-systems-connector-assembly-0.6.7-SNAPSHOT-deps.jar" \
  com.google.cloud.imf.BQExport \
  --billingProject project \
  --projectId project \
  --dataset dataset \
  --table table \
  --paralellism 2 \
  --outputFileType SEQ \
  --destUri gs://bucket/prefix

Export bigquery-public-data sample data to Sequence file: 
java -Xms1g -Xmx1g \
  -cp "open-systems-connector-assembly-0.6.7-SNAPSHOT.jar" \
  com.google.cloud.imf.BQExport \
  --billingProject billingGCPProject \
  --projectId bigquery-public-data  \
  --dataset samples \
  --table shakespeare \
  --paralellism 2 \
  --outputFileType SEQ \
  --destUri gs://wmt-distcp/seqfiles/
```
