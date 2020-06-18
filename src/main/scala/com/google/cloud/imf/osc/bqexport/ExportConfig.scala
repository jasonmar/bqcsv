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

import com.google.cloud.bigquery.storage.v1.ReadSession
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions

case class ExportConfig(billingProjectId: String = "",
                        projectId: String = "",
                        dataset: String = "",
                        table: String = "",
                        filter: String = "",
                        destUri: String = "",
                        paralellism: Int = 1,
                        outputFileType: String = "ORC") {
  def projectPath = s"projects/$billingProjectId"
  def tablePath = s"projects/$projectId/datasets/$dataset/tables/$table"

  def readOpts: ReadSession.TableReadOptions = {
    val builder = TableReadOptions.newBuilder

    if (filter.nonEmpty)
      builder.setRowRestriction(filter)

    builder.build()
  }

  def bucket: String = new java.net.URI(destUri).getAuthority
  def name: String = new java.net.URI(destUri).getPath.stripPrefix("/").stripSuffix("/")
}
