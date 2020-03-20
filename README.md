# BigQuery CSV Uploader

This utility loads a delimited text file into BigQuery with intermediate storage as ORC.

Data pipelines can be simplified by using this utility to import data with the desired type.

Because ORC encoding and compression schemes may reduce upload size by up to 90%,
this tool is useful for reducing upload size in network bandwidth constrained environments.


## How it works

1. User specifies destination schema on command-line
2. Decoders are initialized, one for each field
3. Orc TypeDescription is created from decoders
4. Orc Writer is initialized for initial partition
5. Lines are read from input file and split by delimiter
6. Decoders accept field values, decode and append to Orc ColumnVectors
7. After partition size is reached, a new Orc Writer is created
8. Once input is exhausted, a BigQuery Load Job is submitted


## Building for testing


### Build application jar:

`sbt -Dsbt.log.noformat=true package`


### Build a dependency jar:

`sbt -Dsbt.log.noformat=true assemblyPackageDependency`


### Building assembly jar for single-jar deployment

`sbt -Dsbt.log.noformat=true assembly`


## Usage

Help text:

```
BigQuery CSV Utility
Uploads delimited file to GCS as ORC and loads into BigQuery
Usage: bqcsv [options] source stagingUri tableSpec

  --help               prints this usage text
  --schema <value>     schema information in format <name>[:<type>][:<args>]
example: 'col1:STRING:24,col2:INT64,col3:TIMESTAMP:6,col4:DATE,col5:NUMERIC:9.2'
  --dataset <value>    Default BigQuery dataset in format [PROJECT_ID]:DATASET
  --project <value>    Project ID used for BigQuery requests
  --location <value>   (optional) BigQuery region (default: US)
  --replace            (optional) delete existing ORC file in GCS, if present, and overwrite existing BigQuery table
  --delimiter <value>  (optional) delimiter character
  --debug              (optional) set logging level to debug
  source               path to input file
  stagingUri           GCS prefix where ORC files will be written in format gs://BUCKET/PREFIX
  tableSpec            BigQuery table to be loaded
```


### Run from command line

```sh
java -cp 'target/scala-2.13/bqcsv_2.13-0.1.0-SNAPSHOT.jar:target/scala-2.13/bqcsv.dep.jar' \
  com.google.cloud.imf.BqCsv \
  --replace \
  --delimiter 'þ' \
  --schema 'key1:STRING:24,key2:STRING:24,key3:STRING:24,key4:STRING:24,STATUS:STRING:15,date1:TIMESTAMP,qty1:NUMERIC:14.4,key5:STRING:24,key6:STRING:24,qty2:NUMERIC:14.4,date2:TIMESTAMP,key7:STRING:24,key8:STRING:24,timestamp1:TIMESTAMP,timestamp2:TIMESTAMP,id1:STRING:40,id2:STRING:40,id3:STRING:40,id4:STRING:40,id5:NUMERIC:5.0,rank:TIMESTAMP:6' \
  --dataset dataset \
  --project project \
  path/to/file \
  gs://bucket/prefix \
  project:dataset.table
```


### Providing Schema Information

The utility requires schema information to be provided in order to parse incoming string values.

The syntax is meant to be similar to [`bq load --schema`](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference#bq_load).

Fields are comma-delimited, with optional type arguments for each field delimited by a colon `:`.

This utility accepts an additional type argument following a second `:` delimiter.


#### Example Command-line Option

`--schema 'key1:STRING:24,key2:STRING:24,key3:STRING:24,key4:STRING:24,STATUS:STRING:15,date1:TIMESTAMP,qty1:NUMERIC:14.4,key5:STRING:24,key6:STRING:24,qty2:NUMERIC:14.4,date2:TIMESTAMP,key7:STRING:24,key8:STRING:24,timestamp1:TIMESTAMP,timestamp2:TIMESTAMP,id1:STRING:40,id2:STRING:40,id3:STRING:40,id4:STRING:40,id5:NUMERIC:5.0,rank:TIMESTAMP:6'`


#### DATE fields

use default format `yyyy-MM-dd`

`name:DATE`

specify format `MM/dd/yyyy`

`name:DATE:MM/dd/yyyy`

specify format `yyyyMMdd`

`name:DATE:yyyyMMdd`


#### TIMESTAMP and DATETIME fields

use default format `yyyy-MM-dd HH:mm:ssz`

`name:TIMESTAMP`

use default format `yyyy-MM-dd HH:mm:ss` with timezone `-6`

`name:TIMESTAMP:-6`

specify format `yyyyMMddHHmmss` with timezone `GMT`

`name:TIMESTAMP:0|yyyyMMddHHmmss`

specify format `yyyy-MM-dd HH.mm.ssz` (timezone included)

`name:TIMESTAMP:yyyy-MM-dd HH.mm.ssz`


#### STRING fields

string field

`name`

string field with explicit type

`name:STRING`

string field with maximum length

`name:STRING:length`


#### NUMERIC fields

`NUMERIC` fields require precision and scale arguments separated by `.`

numeric field with precision `9` and scale `2`

`name:NUMERIC:9.2`


#### INT64 fields

`INT64` fields do not require any type arguments

integer field

`name:INT64`


## Disclaimer

This is not an officially supported Google product.
