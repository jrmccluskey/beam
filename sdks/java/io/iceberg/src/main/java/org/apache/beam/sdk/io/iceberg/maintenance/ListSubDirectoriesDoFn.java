/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.iceberg.maintenance;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.FileSystemWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker DoFn that recursively traverses overflowing subdirectories in parallel during {@link
 * DeleteOrphanFiles}.
 */
public class ListSubDirectoriesDoFn extends DoFn<KV<String, String>, KV<String, FileInfo>> {

  private static final Logger LOG = LoggerFactory.getLogger(ListSubDirectoriesDoFn.class);
  private static final int MAX_EXECUTOR_LISTING_DEPTH = 2000;

  private final IcebergCatalogConfig catalogConfig;
  private final DeleteOrphanFiles.Configuration config;

  public ListSubDirectoriesDoFn(
      IcebergCatalogConfig catalogConfig, DeleteOrphanFiles.Configuration config) {
    this.catalogConfig = catalogConfig;
    this.config = config;
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, String> element, OutputReceiver<KV<String, FileInfo>> out) {
    String tableIdString = element.getKey();
    String subDir = element.getValue();

    TableIdentifier tableId = IcebergUtils.parseTableIdentifier(tableIdString);
    Table table = TableCache.getAndRefreshIfStale(catalogConfig, tableId);

    Configuration hadoopConf =
        PlanDeleteOrphanFilesDoFn.getHadoopConfiguration(table, catalogConfig);
    long cutoff = config.olderThanTimestamp();
    Predicate<FileStatus> predicate = file -> file.getModificationTime() < cutoff;

    List<String> files = new ArrayList<>();
    List<String> remainingSubDirs = new ArrayList<>();

    FileSystemWalker.listDirRecursivelyWithHadoop(
        subDir,
        table.specs(),
        predicate,
        hadoopConf,
        MAX_EXECUTOR_LISTING_DEPTH,
        Integer.MAX_VALUE,
        remainingSubDirs::add,
        files::add);

    if (!remainingSubDirs.isEmpty()) {
      LOG.warn(
          DeleteOrphanFiles.PREFIX
              + "Table '{}': subdirectory listing at '{}' exceeded max depth {}. "
              + "{} subdirectories were skipped.",
          tableId,
          subDir,
          MAX_EXECUTOR_LISTING_DEPTH,
          remainingSubDirs.size());
    }

    for (String path : files) {
      String joinKey = DeleteOrphanFiles.makeJoinKey(tableIdString, path);
      FileCategory category = PlanDeleteOrphanFilesDoFn.inferFileCategory(path);
      out.output(KV.of(joinKey, FileInfo.of(path, category, false, tableIdString)));
    }
  }
}
