# BigQuery CSV Utility

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

  --help                   prints this usage text
  --schema <value>         (optional) schema information in format <name>[:<type>][:<args>],...
example: 'col1:STRING:24,col2:INT64,col3:TIMESTAMP:6,col4:DATE,col5:NUMERIC:9.2'
  --project <value>        Project ID used for BigQuery requests
  --dataset <value>        Default BigQuery Dataset in format [PROJECT_ID]:DATASET
  --location <value>       (optional) BigQuery region (default: US)
  --lifetime <value>       (optional) table lifetime in milliseconds (default: 7 days)
  --replace                (optional) delete existing ORC file in GCS, if present, and overwrite existing BigQuery table
  --append                 (optional) append to BigQuery table
  --external               (optional) register as BigQuery External Table instead of loading
  --delimiter <value>      (optional) delimiter character
  --templateTableSpec <value>
                           (optional) TableSpec of BigQuery table to use as schema template in format [project:][dataset:]table
  --debug                  (optional) set logging level to debug
  source                   path to input file
  stagingUri               GCS prefix where ORC files will be written in format gs://BUCKET/PREFIX
  tableSpec                BigQuery table to be loaded in format [project:][dataset:]table
```


### Command line example: load table with schema specified by command-line option

```sh
java -cp 'target/scala-2.13/bqcsv_2.13-0.1.0-SNAPSHOT.jar:target/scala-2.13/bqcsv.dep.jar' \
  com.google.cloud.imf.BqCsv \
  --delimiter 'þ' \
  --schema 'key:STRING:24,date1:TIMESTAMP,qty:NUMERIC:14.4,id1:STRING,pct:FLOAT64,ts2:TIMESTAMP:-8' \
  --dataset dataset \
  --project project \
  path/to/file \
  gs://bucket/prefix \
  project:dataset.table
```

### Command line example: register as external table with lifetime of 1 hour using schema template

```
java -cp "target/scala-2.13/bqcsv_2.13-0.1.0-SNAPSHOT.jar:target/scala-2.13/bqcsv.dep.jar" \
  com.google.cloud.imf.BqCsv \
  --external \
  --lifetime 3600000 \
  --dataset dataset \
  --project project \
  --templateTableSpec project:dataset.template \
  path/to/file \
  gs://bucket/prefix \
  project:dataset.table
```


### Providing Schema Information

The utility requires schema information to be provided in order to parse incoming string values.

Schema can be provided via template table or command-line option.


#### Providing Schema Information via Template BigQuery Table

Users may prefer to simplify command-line arguments by adding type arguments to 
column descriptions in a BigQuery table used as a schema template.

The template table could be the destination table or it could be an empty table 
in a dataset designated specifically as a template dataset.


##### Type arguments

* `TIMESTAMP`: `<format>` (optional) (see [DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html))
* `TIMESTAMP`: `<offset>` (optional) (see [ZoneOffset](https://docs.oracle.com/javase/8/docs/api/java/time/ZoneOffset.html))
* `TIMESTAMP`: `<offset>|<format>` (optional) 
* `NUMERIC`: `<precision>,<scale>` (required)
* `STRING`: `<length>` (optional)
* `DATETIME`: same as `TIMESTAMP`

If a format is not specified
* `TIMESTAMP` uses format `yyyy-MM-dd HH:mm:ssz`
* `DATE` uses format `yyyy-MM-dd`

If a timestamp does not include a timezone, 
an offset argument must be provided.


##### StandardSQL Types without arguments

* STRING
* INT64
* FLOAT64

##### DDL for template table with type arguments in column description

```
CREATE TABLE dataset.template (
    key string,
    ts1 TIMESTAMP OPTIONS(description="yyyy-MM-dd HH:mm:ssz"),
    qty NUMERIC OPTIONS(description="14,4"),
    id1 string,
    id2 string,
    id3 NUMERIC OPTIONS(description="5,0"),
    ts2 TIMESTAMP OPTIONS(description="6|yyyy-MM-dd HH:mm:ss")
)
```


#### Providing Schema Information via Command-line Option

The syntax is meant to be similar to [`bq load --schema`](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference#bq_load).

Fields are comma-delimited, with optional type arguments for each field delimited by a colon `:`.

This utility accepts an additional type argument following a second `:` delimiter.

Example:

`--schema 'key:STRING:24,date1:TIMESTAMP,qty:NUMERIC:14.4,id1:STRING,pct:FLOAT64,ts2:TIMESTAMP:-8'`


##### DATE fields

use default format `yyyy-MM-dd`

`name:DATE`

specify format `MM/dd/yyyy`

`name:DATE:MM/dd/yyyy`

specify format `yyyyMMdd`

`name:DATE:yyyyMMdd`


##### TIMESTAMP and DATETIME fields

use default format `yyyy-MM-dd HH:mm:ssz`

`name:TIMESTAMP`

use default format `yyyy-MM-dd HH:mm:ss` with timezone `-6`

`name:TIMESTAMP:-6`

specify format `yyyyMMddHHmmss` with timezone `GMT`

`name:TIMESTAMP:0|yyyyMMddHHmmss`

specify format `yyyy-MM-dd HH.mm.ssz` (timezone included)

`name:TIMESTAMP:yyyy-MM-dd HH.mm.ssz`


##### STRING fields

string field

`name`

string field with explicit type

`name:STRING`

string field with maximum length

`name:STRING:length`


##### NUMERIC fields

`NUMERIC` fields require precision and scale arguments separated by `.`

numeric field with precision `9` and scale `2`

`name:NUMERIC:9.2`


##### INT64 fields

`INT64` fields do not require any type arguments

integer field

`name:INT64`


##### FLOAT64 fields

`FLOAT64` fields do not require any type arguments

double field

`name:FLOAT64`


## Disclaimer

This is not an officially supported Google product.
