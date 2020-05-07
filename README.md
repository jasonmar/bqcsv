# BigQuery Open Systems Connector

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
Google Cloud Open Systems Connector

*  Uploads delimited files to GCS as ORC
*  Registers ORC as external table or Loads into Native BigQuery table

Usage: OSC [options] stagingUri tableSpec source

  --help                   prints this usage text
  --schema <value>         (optional) schema information in format <name>[:<type>][:<args>],...
example: 'col1:STRING:24,col2:INT64,col3:TIMESTAMP:6,col4:DATE,col5:NUMERIC:9.2'
  --project <value>        Project ID used for BigQuery requests
  --dataset <value>        Default BigQuery Dataset in format [PROJECT_ID]:DATASET
  --location <value>       (optional) BigQuery region (default: US)
  --lifetime <value>       (optional) table lifetime in milliseconds (default: 7 days)
  --sampleSize <value>     (optional) number of rows to sample for schema inference (default: 4096)
  --replace                (optional) delete existing ORC file in GCS, if present, and overwrite existing BigQuery table
  --append                 (optional) append to BigQuery table
  --external               (optional) register as BigQuery External Table instead of loading
  --autodetect             (optional) infer schema from first 100 lines of file
  --zoneId <value>         (optional) time zone ID https://www.iana.org/time-zones (default: America/Los_Angeles)
  --parallelism <value>    (optional) parallelism (default: 1)
  --errorLimit <value>     (optional) maximum number of errors per thread (default: 0)
  --delimiter <value>      (optional) delimiter character (default: þ)
  --templateTableSpec <value>
                           (optional) TableSpec of BigQuery table to use as schema template in format [project:][dataset:]table
  --debug                  (optional) set logging level to debug
  stagingUri               GCS prefix where ORC files will be written in format gs://BUCKET/PREFIX
  tableSpec                BigQuery table to be loaded in format project:dataset.table
  source                   Path to input file (can be provided multiple times)
```

### Multiple input files

If the `source` argument is provided multiple times,
lines from all source files will be concatenated and
treated as a single source.


### Command line example: load table with schema specified by command-line option

```sh
lib='target/scala-2.13'
cp="$lib/open-systems-connector_2.13-<version>
.jar:$lib/open-systems-connector-assembly-<version>-deps.jar"
java -cp "$cp" com.google.cloud.imf.OSC \
  --delimiter 'þ' \
  --autodetect \
  --dataset dataset \
  --project project \
  gs://bucket/prefix \
  project:dataset.table \
  path/to/file
```

### Command line example: register as external table with lifetime of 1 hour using schema template

```
lib='target/scala-2.13'
cp="$lib/open-systems-connector_2.13-<version>
.jar:$lib/open-systems-connector-assembly-<version>-deps.jar"
java -cp "$cp" com.google.cloud.imf.OSC \
  --external \
  --lifetime 3600000 \
  --dataset dataset \
  --project project \
  --templateTableSpec project:dataset.template \
  gs://bucket/prefix \
  project:dataset.table \
  path/to/file
```


### Providing Schema Information

The utility requires schema information to be provided in order to parse incoming string values.

Schema can be obtained via autodetection,  template table or command-line option.


#### Schema Autodetection

Pass the autodetect flag to enable schema autodetection:

`--autodetect`

If you pass a template table spec, the utility will attempt to obtain field names from it

`--templateTableSpec project:dataset.table`

You may also want to pass column names via the schema option:

`--schema colName1,colName2,...`

If no template table or schema is provided, the utility will attempt to obtain field names from the destination table.

If none of the above methods succeed, column names will be autogenerated using spreadsheet column naming convention


#### Providing Schema Information via Template BigQuery Table

Users may prefer to simplify command-line arguments by adding type arguments to
column descriptions in a BigQuery table used as a schema template.

The template table could be the destination table or it could be an empty table
in a dataset designated specifically as a template dataset.


##### Type arguments

* `TIMESTAMP`: `<format>` (optional) (see [DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html))
* `TIMESTAMP`: `<tzId>` (optional) (see [ZoneOffset](https://docs.oracle.com/javase/8/docs/api/java/time/ZoneOffset.html))
* `TIMESTAMP`: `<tzId>|<format>` (optional)
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
    ts2 TIMESTAMP OPTIONS(description="America/Chicago|yyyy-MM-dd HH:mm:ss")
)
```


#### Providing Schema Information via Command-line Option

The syntax is meant to be similar to [`bq load --schema`](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference#bq_load).

Fields are comma-delimited, with optional type arguments for each field delimited by a colon `:`.

This utility accepts an additional type argument following a second `:` delimiter.

Example:

`--schema 'key:STRING:24,date1:TIMESTAMP,qty:NUMERIC:14.4,id1:STRING,pct:FLOAT64,ts2:TIMESTAMP:America/Chicago'`


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

use default format `yyyy-MM-dd HH:mm:ss` with timezone `America/Chicago`

`name:TIMESTAMP:America/Chicago`

specify format `yyyyMMddHHmmss` with timezone `America/Chicago`

`name:TIMESTAMP:America/Chicago|yyyyMMddHHmmss`

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


## Working with TIMESTAMP columns

All timestamps are stored as UTC.
In order to view a timestamp in a different timezone,
use the [STRING Standard SQL Function](https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#string)

Example:

```
SELECT STRING(TIMESTAMP "2020-03-26 15:30:00", "America/Chicago") as string;

+-------------------------------+
| string                        |
+-------------------------------+
| 2020-03-26 07:30:00-08        |
+-------------------------------+
```


## Disclaimer

This is not an officially supported Google product.
