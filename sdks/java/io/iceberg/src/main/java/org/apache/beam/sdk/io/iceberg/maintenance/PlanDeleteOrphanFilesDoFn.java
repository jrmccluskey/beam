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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.util.FileSystemWalker;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execution-time planning DoFn for {@link DeleteOrphanFiles}.
 *
 * <p>Validates {@code GC_ENABLED}, extracts all reachable metadata references (snapshots,
 * manifests, manifest lists, metadata JSON logs, statistics), and performs initial recursive
 * storage listing.
 */
public class PlanDeleteOrphanFilesDoFn extends DoFn<String, DeleteOrphanFilesResult> {

  private static final Logger LOG = LoggerFactory.getLogger(PlanDeleteOrphanFilesDoFn.class);

  public static final TupleTag<DeleteOrphanFilesResult> PLAN_SUMMARY = new TupleTag<>() {};
  public static final TupleTag<KV<String, FileInfo>> METADATA_FILES = new TupleTag<>() {};
  public static final TupleTag<KV<String, ManifestFileBean>> MANIFESTS = new TupleTag<>() {};
  public static final TupleTag<KV<String, String>> SUB_DIRECTORIES = new TupleTag<>() {};
  public static final TupleTag<KV<String, FileInfo>> STORAGE_FILES = new TupleTag<>() {};

  private final IcebergCatalogConfig catalogConfig;
  private final DeleteOrphanFiles.Configuration config;

  public PlanDeleteOrphanFilesDoFn(
      IcebergCatalogConfig catalogConfig, DeleteOrphanFiles.Configuration config) {
    this.catalogConfig = catalogConfig;
    this.config = config;
  }

  @ProcessElement
  public void processElement(@Element String tableIdString, MultiOutputReceiver out)
      throws IOException {
    TableIdentifier tableId = IcebergUtils.parseTableIdentifier(tableIdString);
    Table table = TableCache.getAndRefreshIfStale(catalogConfig, tableId);

    boolean gcEnabled =
        PropertyUtil.propertyAsBoolean(
            table.properties(), TableProperties.GC_ENABLED, TableProperties.GC_ENABLED_DEFAULT);
    if (!gcEnabled) {
      throw new ValidationException(
          "Cannot delete orphan files: GC is disabled (deleting files may corrupt other tables)");
    }

    out.get(PLAN_SUMMARY).output(DeleteOrphanFilesResult.zeros());

    // 1. Gather Valid Manifest Lists across all snapshots
    for (String manifestList : ReachableFileUtil.manifestListLocations(table)) {
      if (manifestList != null) {
        String joinKey = DeleteOrphanFiles.makeJoinKey(tableIdString, manifestList);
        out.get(METADATA_FILES)
            .output(
                KV.of(
                    joinKey,
                    FileInfo.of(manifestList, FileCategory.MANIFEST_LIST, true, tableIdString)));
      }
    }

    // 2. Gather Valid Metadata JSON Files (current and historical chained metadata)
    for (String metadataFile : ReachableFileUtil.metadataFileLocations(table, true)) {
      if (metadataFile != null) {
        String joinKey = DeleteOrphanFiles.makeJoinKey(tableIdString, metadataFile);
        out.get(METADATA_FILES)
            .output(
                KV.of(
                    joinKey,
                    FileInfo.of(metadataFile, FileCategory.OTHER_METADATA, true, tableIdString)));
      }
    }

    // 3. Gather Version Hint File
    String versionHint = ReachableFileUtil.versionHintLocation(table);
    if (versionHint != null) {
      String joinKey = DeleteOrphanFiles.makeJoinKey(tableIdString, versionHint);
      out.get(METADATA_FILES)
          .output(
              KV.of(
                  joinKey,
                  FileInfo.of(versionHint, FileCategory.OTHER_METADATA, true, tableIdString)));
    }

    // 4. Gather Statistics Files
    for (String statsFile : ReachableFileUtil.statisticsFilesLocations(table)) {
      if (statsFile != null) {
        String joinKey = DeleteOrphanFiles.makeJoinKey(tableIdString, statsFile);
        out.get(METADATA_FILES)
            .output(
                KV.of(
                    joinKey, FileInfo.of(statsFile, FileCategory.STATISTICS, true, tableIdString)));
      }
    }

    // 5. Gather Partition Statistics Files
    if (table.partitionStatisticsFiles() != null) {
      for (PartitionStatisticsFile psf : table.partitionStatisticsFiles()) {
        if (psf != null && psf.path() != null) {
          String joinKey = DeleteOrphanFiles.makeJoinKey(tableIdString, psf.path());
          out.get(METADATA_FILES)
              .output(
                  KV.of(
                      joinKey,
                      FileInfo.of(psf.path(), FileCategory.STATISTICS, true, tableIdString)));
        }
      }
    }

    // 6. Gather Manifest Files across all snapshots
    Set<String> seenManifestPaths = new HashSet<>();
    for (Snapshot s : table.snapshots()) {
      for (ManifestFile m : s.allManifests(table.io())) {
        if (seenManifestPaths.add(m.path())) {
          String joinKey = DeleteOrphanFiles.makeJoinKey(tableIdString, m.path());
          out.get(METADATA_FILES)
              .output(
                  KV.of(
                      joinKey, FileInfo.of(m.path(), FileCategory.MANIFEST, true, tableIdString)));

          out.get(MANIFESTS)
              .output(KV.of(tableIdString, ManifestFileBean.fromManifestFile(m, true)));
        }
      }
    }

    // 7. Physical Storage Listing
    String loc = config.location() != null ? config.location() : table.location();
    String scanLocation = Preconditions.checkNotNull(loc, "Scan location must not be null");
    long cutoff = config.olderThanTimestamp();

    List<String> matchingFiles = new ArrayList<>();
    List<String> subDirs = new ArrayList<>();

    boolean canUsePrefixListing =
        config.usePrefixListing() && table.io() instanceof SupportsPrefixOperations;
    if (config.usePrefixListing() && !(table.io() instanceof SupportsPrefixOperations)) {
      LOG.warn(
          DeleteOrphanFiles.PREFIX
              + "Prefix listing requested for table '{}', but FileIO does not implement SupportsPrefixOperations. Falling back to directory walk.",
          tableId);
    }

    if (canUsePrefixListing) {
      Predicate<org.apache.iceberg.io.FileInfo> predicate =
          fileInfo -> fileInfo.createdAtMillis() < cutoff;
      FileSystemWalker.listDirRecursivelyWithFileIO(
          (SupportsPrefixOperations) table.io(),
          scanLocation,
          table.specs(),
          predicate,
          matchingFiles::add);
    } else {
      Configuration hadoopConf = getHadoopConfiguration(table, catalogConfig);
      Predicate<FileStatus> predicate = file -> file.getModificationTime() < cutoff;
      FileSystemWalker.listDirRecursivelyWithHadoop(
          scanLocation,
          table.specs(),
          predicate,
          hadoopConf,
          config.subDirectoryListingDepth(),
          config.subDirectoryDirectSubDirs(),
          subDirs::add,
          matchingFiles::add);
    }

    for (String path : matchingFiles) {
      String joinKey = DeleteOrphanFiles.makeJoinKey(tableIdString, path);
      FileCategory category = inferFileCategory(path);
      out.get(STORAGE_FILES)
          .output(KV.of(joinKey, FileInfo.of(path, category, false, tableIdString)));
    }

    for (String subDir : subDirs) {
      out.get(SUB_DIRECTORIES).output(KV.of(tableIdString, subDir));
    }

    LOG.info(
        DeleteOrphanFiles.PREFIX
            + "Table '{}': identified {} valid manifest(s), {} shallow storage file(s), {} subdirectory branch(es).",
        tableId,
        seenManifestPaths.size(),
        matchingFiles.size(),
        subDirs.size());
  }

  /** Resolves Hadoop configuration from {@link HadoopFileIO} or catalog config properties. */
  public static Configuration getHadoopConfiguration(
      Table table, IcebergCatalogConfig catalogConfig) {
    if (table.io() instanceof HadoopFileIO) {
      return ((HadoopFileIO) table.io()).getConf();
    }
    Configuration conf = new Configuration();
    if (catalogConfig.getConfigProperties() != null) {
      for (Map.Entry<String, String> entry : catalogConfig.getConfigProperties().entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
    if (catalogConfig.getCatalogProperties() != null) {
      for (Map.Entry<String, String> entry : catalogConfig.getCatalogProperties().entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
    return conf;
  }

  /** Categorizes physical storage files by inspecting standard Iceberg naming conventions. */
  public static FileCategory inferFileCategory(String path) {
    if (path == null) {
      return FileCategory.UNKNOWN;
    }
    String lower = path.toLowerCase(Locale.ROOT);
    if (lower.endsWith(".metadata.json") || lower.endsWith("version-hint.text")) {
      return FileCategory.OTHER_METADATA;
    }
    if (lower.endsWith(".puffin")) {
      return FileCategory.STATISTICS;
    }
    if (lower.contains("snap-") && lower.endsWith(".avro")) {
      return FileCategory.MANIFEST_LIST;
    }
    if (lower.endsWith(".avro") && lower.contains("/metadata/")) {
      return FileCategory.MANIFEST;
    }
    if (lower.contains("equality-delete")) {
      return FileCategory.EQUALITY_DELETES;
    }
    if (lower.contains("-deletes.") || lower.contains("position-delete")) {
      return FileCategory.POSITION_DELETES;
    }
    if (lower.endsWith(".parquet") || lower.endsWith(".orc") || lower.endsWith(".avro")) {
      return FileCategory.DATA;
    }
    return FileCategory.UNKNOWN;
  }
}
