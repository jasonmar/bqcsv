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

import com.google.cloud.bigquery.{StandardSQLTypeName, TableId}

trait TableDefProvider {
  /** Get a map used to look up Type for a given field
   * @param tbl
   * @return Map from lowercase field name to SQL Type
   */
  def getFieldMap(tbl: TableId): Map[String,StandardSQLTypeName]

  /** Get tuples used to look up Type for a given field
   * @param tbl id of tabledef to be fetched
   * @return Tuple of lowercase field name and SQL Type
   */
  def getFieldMap2(tbl: TableId): Seq[(String,StandardSQLTypeName)]
}
