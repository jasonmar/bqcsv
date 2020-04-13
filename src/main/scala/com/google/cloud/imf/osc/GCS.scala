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

import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.auth.Credentials
import com.google.cloud.storage.{Storage, StorageOptions}

object GCS extends Logging {
  def defaultClient(credentials: Credentials): Storage = {
    StorageOptions.newBuilder
      .setCredentials(credentials)
      .setHeaderProvider(FixedHeaderProvider.create("user-agent", BQ.UserAgent))
      .build
      .getService
  }

  def assertEmpty(gcs: Storage, uri: java.net.URI): Unit = {
    logger.debug(s"Deleting recursively from $uri")
    val withTrailingSlash = uri.getPath.stripPrefix("/") + (if (uri.getPath.last == '/') "" else "/")
    val bucket = uri.getAuthority
    val ls = gcs.list(bucket,
      Storage.BlobListOption.prefix(withTrailingSlash),
      Storage.BlobListOption.currentDirectory)
    import scala.jdk.CollectionConverters.IterableHasAsScala
    assert(ls == null || ls.iterateAll.asScala.isEmpty, s"$uri is not empty")
  }

  def delete(gcs: Storage, uri: java.net.URI): Unit = {
    logger.debug(s"Deleting recursively from $uri")
    val withTrailingSlash = uri.getPath.stripPrefix("/") + (if (uri.getPath.last == '/') "" else "/")
    val bucket = uri.getAuthority
    var ls = gcs.list(bucket,
      Storage.BlobListOption.prefix(withTrailingSlash),
      Storage.BlobListOption.currentDirectory)
    import scala.jdk.CollectionConverters.IterableHasAsScala
    var deleted: Long = 0
    var notDeleted: Long = 0
    while (ls != null) {
      val blobIds = ls.getValues.asScala.toArray.map(_.getBlobId)
      if (blobIds.nonEmpty){
        logger.debug(s"deleting ${blobIds.map(_.getName).mkString(",")}")
        val deleteResults = gcs.delete(blobIds: _*)
        deleted += deleteResults.asScala.count(_ == true)
        notDeleted += deleteResults.asScala.count(_ == false)
      }
      ls = ls.getNextPage
    }
    logger.debug(s"deleted $deleted and failed to delete $notDeleted")
  }
}
