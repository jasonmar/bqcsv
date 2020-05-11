

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
java -Xms512m -Xmx512m \
  -cp "open-systems-connector_2.13-0.6.0-SNAPSHOT.jar:open-systems-connector-assembly-0.6.0-SNAPSHOT-deps.jar"
  --billingProject project \
  --projectId project \
  --dataset dataset \
  --table table \
  --paralellism 2 \
  --destUri gs://bucket/prefix
```
