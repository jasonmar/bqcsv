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

package com.google.cloud.imf.osc.bqexport

import scopt.OptionParser

object ExportConfigParser
  extends OptionParser[ExportConfig]("com.google.cloud.imf.BQExport"){
  head("com.google.cloud.imf.BQExport", "0.6")
    .text("""Google Cloud Open Systems Connector
            |
            |*  BQExport - Exports BigQuery table to GCS as CSV
            |""".stripMargin)

  help("help").text("prints this usage text")

  opt[String]("billingProject")
    .required
    .text("Billing project ID")
    .action{(x,c) => c.copy(billingProjectId = x)}

  opt[String]("projectId")
    .required
    .text("Project ID")
    .action{(x,c) => c.copy(projectId = x)}

  opt[String]("dataset")
    .required
    .text("Source Dataset")
    .action{(x,c) => c.copy(dataset = x)}

  opt[String]("table")
    .required
    .text("Source Table")
    .action{(x,c) => c.copy(table = x)}

  opt[String]("destUri")
    .required
    .text("Destination URI")
    .action{(x,c) => c.copy(destUri = x)}

  opt[Int]("paralellism")
    .optional
    .text("Number of Threads")
    .action{(x,c) => c.copy(paralellism = x)}

  opt[String]("outputFileType")
    .optional
    .text("Output file type ORC or SEQ")
    .action{(x,c) => c.copy(outputFileType = x)}
}
