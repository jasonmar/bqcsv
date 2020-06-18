package com.google.cloud.imf.osc.bqexport

import com.google.cloud.bigquery.storage.v1.AvroRows

trait Export {
  def processRows(rows: AvroRows): Long
  def close(): Unit
}
