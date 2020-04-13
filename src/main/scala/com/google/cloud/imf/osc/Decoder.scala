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

import com.google.cloud.bigquery.StandardSQLTypeName
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector
import org.apache.orc.TypeDescription

trait Decoder {
  /** Read a field into a mutable output builder
    *
    * @param s String
    * @param column ColumnVector
    * @param i row index
    */
  def get(s: String, column: ColumnVector, i: Int): Unit

  def columnVector(maxSize: Int): ColumnVector

  def typeDescription: TypeDescription

  def bqType: StandardSQLTypeName
}
