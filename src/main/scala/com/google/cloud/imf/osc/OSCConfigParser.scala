/*
 * Copyright 2020 Google LLC All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.imf.osc

import java.net.URI
import java.nio.file.{Files, Paths}

import scopt.OptionParser

import scala.collection.immutable.ArraySeq


object OSCConfigParser extends OptionParser[OSCConfig]("OSC") {
  def parse(args: Array[String]): Option[OSCConfig] = parse(ArraySeq.unsafeWrapArray(args), OSCConfig())

  head("osc", "0.6")
    .text("""Google Cloud Open Systems Connector
            |
            |*  Uploads delimited files to GCS as ORC
            |*  Registers ORC as external table or Loads into Native BigQuery table
            |""".stripMargin)

  help("help").text("prints this usage text")

  opt[String]("schema")
    .optional
    .text("(optional) schema information in format <name>[:<type>][:<args>],...\nexample: 'col1:STRING:24,col2:INT64,col3:TIMESTAMP:6,col4:DATE,col5:NUMERIC:9.2'")
    .action{(x,c) => c.copy(schema = x)}

  opt[String]("project")
    .required
    .text("Project ID used for BigQuery requests")
    .action((x,c) => c.copy(projectId = x))

  // BigQuery Options
  opt[String]("dataset")
    .required
    .text("Default BigQuery Dataset in format [PROJECT_ID]:DATASET")
    .action((x,c) => c.copy(datasetId = x))

  opt[String]("location")
    .optional
    .text("(optional) BigQuery region (default: US)")
    .action((x,c) => c.copy(location = x))

  opt[Long]("lifetime")
    .optional
    .text("(optional) table lifetime in milliseconds (default: 7 days)")
    .action((x,c) => c.copy(lifetime = x))

  opt[Int]("sampleSize")
    .optional
    .text("(optional) number of rows to sample for schema inference (default: 4096)")
    .action((x,c) => c.copy(sampleSize = x))

  opt[Unit]("replace")
    .optional
    .action{(_,c) => c.copy(replace = true)}
    .text("(optional) delete existing ORC file in GCS, if present, and overwrite existing BigQuery table")

  opt[Unit]("append")
    .optional
    .action{(_,c) => c.copy(append = true)}
    .text("(optional) append to BigQuery table")

  opt[Unit]("external")
    .optional
    .action{(_,c) => c.copy(external = true)}
    .text("(optional) register as BigQuery External Table instead of loading")

  opt[Unit]("autodetect")
    .optional
    .action{(_,c) => c.copy(autodetect = true)}
    .text("(optional) infer schema from first 100 lines of file")

  opt[String]("zoneId")
    .optional
    .action{(x,c) => c.copy(zoneId = x)}
    .text("(optional) time zone ID https://www.iana.org/time-zones (default: America/Los_Angeles)")

  opt[Int]("parallelism")
    .optional
    .action{(x,c) => c.copy(parallelism = x)}
    .text("(optional) parallelism (default: 1)")
    .validate(x => if (x > 0) success else failure("parallelism must be positive"))

  opt[Int]("errorLimit")
    .optional
    .action{(x,c) => c.copy(errorLimit = x)}
    .text("(optional) maximum number of errors per thread (default: 0)")

  opt[String]("delimiter")
    .optional
    .text("(optional) delimiter character (default: þ)")
    .action{(x,c) => c.copy(delimiter = x.head)}

  opt[String]("templateTableSpec")
    .optional
    .text("(optional) TableSpec of BigQuery table to use as schema template in format [project:][dataset:]table")
    .action{(x,c) => c.copy(templateTableSpec = x)}

  opt[Unit]("debug")
    .optional
    .text("(optional) set logging level to debug")
    .action((_,c) => c.copy(debug = true))

  arg[String]("stagingUri")
    .required
    .text("GCS prefix where ORC files will be written in format gs://BUCKET/PREFIX")
    .validate{x =>
      val uri = new URI(x)
      if (uri.getScheme != "gs" || uri.getAuthority.isEmpty)
        failure("invalid GCS URI")
      else
        success
    }
    .action((x, c) => c.copy(stagingUri = x))

  arg[String]("tableSpec")
    .required
    .text("BigQuery table to be loaded in format project:dataset.table")
    .validate(x =>
      if (x.length < 3) failure(s"invalid tablespec $x")
      else success
    )
    .action((x, c) => c.copy(destTableSpec = x))

  arg[String]("source")
    .required
    .maxOccurs(1024)
    .text("Path to input file (can be provided multiple times)")
    .validate{x =>
      val paths = if (x.contains(',')){
        x.split(',').toIndexedSeq
      } else Seq(x)
      if (paths.forall{path => Files.isRegularFile(Paths.get(path))})
        success
      else
        failure(s"source not found")
    }
    .action{(x, c) =>
      val paths = if (x.contains(',')) x.split(',').toIndexedSeq else Seq(x)
      c.copy(source = c.source ++ paths)
    }
}
