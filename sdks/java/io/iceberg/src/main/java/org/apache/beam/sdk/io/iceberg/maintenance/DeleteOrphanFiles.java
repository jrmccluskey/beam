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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.actions.DeleteOrphanFiles.PrefixMismatchMode;
import org.apache.iceberg.actions.FileURI;
import org.apache.iceberg.exceptions.ValidationException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

/**
 * Distributed orphan files cleanup maintenance operation for Apache Iceberg tables.
 *
 * <p>Discovers and removes physical data, delete, manifest, and metadata files from underlying
 * storage that are no longer referenced by any valid snapshot or metadata tree.
 *
 * <h2>Execution Model</h2>
 *
 * <ol>
 *   <li><b>Phase 1 (Planning &amp; Discovery)</b>: Worker {@link PlanDeleteOrphanFilesDoFn}
 *       validates GC settings, extracts all reachable metadata references (snapshots, manifests,
 *       manifest lists, metadata JSON logs, statistics), and begins recursive storage listing.
 *   <li><b>Phase 2 (Distributed Scanning)</b>: Manifests and deep subdirectories are fanned out and
 *       processed in parallel across workers via {@link ReadManifestDoFn} and {@link
 *       ListSubDirectoriesDoFn}.
 *   <li><b>Phase 3 (Anti-Join &amp; URI Normalization)</b>: Actual storage files and metadata
 *       entries are keyed by canonical path, joined in a distributed anti-join, and evaluated
 *       according to {@link PrefixMismatchMode} and equal scheme/authority mappings.
 *   <li><b>Phase 4 (Batched Deletion)</b>: Unreferenced files older than the safety cutoff are
 *       batched and deleted in parallel across workers via key-sharded {@link GroupIntoBatches} and
 *       {@link DeleteOrphanFilesDoFn}, utilizing bulk object deletion where supported.
 *   <li><b>Phase 5 (Aggregation)</b>: Deletion counts are merged globally into a final {@link
 *       DeleteOrphanFilesResult}.
 * </ol>
 */
public class DeleteOrphanFiles
    extends PTransform<PCollection<String>, PCollection<DeleteOrphanFilesResult>> {

  public static final String PREFIX = "[DeleteOrphanFiles] ";
  private static final Map<String, String> EQUAL_SCHEMES_DEFAULT = ImmutableMap.of("s3n,s3a", "s3");
  private static final Splitter COMMA_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();

  private final IcebergCatalogConfig catalogConfig;
  private final Configuration config;

  DeleteOrphanFiles(IcebergCatalogConfig catalogConfig, Configuration config) {
    this.catalogConfig = catalogConfig;
    this.config = config;
  }

  public static DeleteOrphanFiles create(IcebergCatalogConfig catalogConfig) {
    return new DeleteOrphanFiles(catalogConfig, Configuration.builder().build());
  }

  public static DeleteOrphanFiles create(IcebergCatalogConfig catalogConfig, Configuration config) {
    return new DeleteOrphanFiles(catalogConfig, config);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.addIfNotNull(
        DisplayData.item("location", config.getLocation()).withLabel("Scan Location"));
    builder.addIfNotNull(
        DisplayData.item("olderThanTimestamp", config.getOlderThanTimestamp())
            .withLabel("Older Than (Millis)"));
    builder.add(
        DisplayData.item("cleanFiles", config.cleanFiles())
            .withLabel("Clean Files (Physical Deletion)"));
    builder.add(
        DisplayData.item("deleteBatchSize", config.deleteBatchSize())
            .withLabel("Delete Batch Size"));
    builder.add(
        DisplayData.item("prefixMismatchMode", config.prefixMismatchMode().name())
            .withLabel("Prefix Mismatch Mode"));
    builder.add(
        DisplayData.item("usePrefixListing", config.usePrefixListing())
            .withLabel("Use Prefix Listing"));
  }

  @Override
  public PCollection<DeleteOrphanFilesResult> expand(PCollection<String> tableIdentifiers) {
    Preconditions.checkArgument(
        tableIdentifiers.isBounded() == PCollection.IsBounded.BOUNDED,
        "DeleteOrphanFiles only supports bounded (batch) input.");
    config.validate();

    SchemaCoder<FileInfo> fileInfoCoder;
    SchemaCoder<DeleteOrphanFilesResult> resultCoder;
    try {
      fileInfoCoder =
          tableIdentifiers.getPipeline().getSchemaRegistry().getSchemaCoder(FileInfo.class);
      resultCoder =
          tableIdentifiers
              .getPipeline()
              .getSchemaRegistry()
              .getSchemaCoder(DeleteOrphanFilesResult.class);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException("Failed to load schema coders for DeleteOrphanFiles", e);
    }

    KvCoder<String, FileInfo> kvFileInfoCoder = KvCoder.of(StringUtf8Coder.of(), fileInfoCoder);

    // Phase 1: Planning and Discovery
    PCollectionTuple planned =
        tableIdentifiers.apply(
            "Plan Delete Orphan Files",
            ParDo.of(new PlanDeleteOrphanFilesDoFn(catalogConfig, config))
                .withOutputTags(
                    PlanDeleteOrphanFilesDoFn.PLAN_SUMMARY,
                    TupleTagList.of(PlanDeleteOrphanFilesDoFn.METADATA_FILES)
                        .and(PlanDeleteOrphanFilesDoFn.MANIFESTS)
                        .and(PlanDeleteOrphanFilesDoFn.SUB_DIRECTORIES)
                        .and(PlanDeleteOrphanFilesDoFn.STORAGE_FILES)));

    PCollection<DeleteOrphanFilesResult> planSummary =
        planned.get(PlanDeleteOrphanFilesDoFn.PLAN_SUMMARY).setCoder(resultCoder);

    PCollection<KV<String, FileInfo>> metadataFiles =
        planned.get(PlanDeleteOrphanFilesDoFn.METADATA_FILES).setCoder(kvFileInfoCoder);

    PCollection<KV<String, FileInfo>> shallowStorageFiles =
        planned.get(PlanDeleteOrphanFilesDoFn.STORAGE_FILES).setCoder(kvFileInfoCoder);

    // Phase 2: Distributed Manifest Scanning
    PCollection<KV<String, FileInfo>> manifestEntries =
        planned
            .get(PlanDeleteOrphanFilesDoFn.MANIFESTS)
            .setCoder(
                KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(ManifestFileBean.class)))
            .apply("Redistribute Manifests", Redistribute.arbitrarily())
            .apply("Read Manifest Entries", ParDo.of(new ReadManifestDoFn(catalogConfig)))
            .apply(
                "Key Manifest Entries",
                MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptors.strings(), TypeDescriptor.of(FileInfo.class)))
                    .via(
                        kv ->
                            KV.of(
                                makeJoinKey(
                                    kv.getValue().getTableIdentifier(), kv.getValue().getPath()),
                                kv.getValue())))
            .setCoder(kvFileInfoCoder);

    // Phase 2b: Distributed Subdirectory Listing
    PCollection<KV<String, FileInfo>> subDirectoryFiles =
        planned
            .get(PlanDeleteOrphanFilesDoFn.SUB_DIRECTORIES)
            .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .apply("Redistribute Subdirectories", Redistribute.arbitrarily())
            .apply(
                "List Subdirectories", ParDo.of(new ListSubDirectoriesDoFn(catalogConfig, config)))
            .setCoder(kvFileInfoCoder);

    // Phase 3: Anti-Join & URI Normalization
    PCollection<FileInfo> orphanFiles =
        PCollectionList.of(metadataFiles)
            .and(manifestEntries)
            .and(shallowStorageFiles)
            .and(subDirectoryFiles)
            .apply("Flatten All Entries", Flatten.pCollections())
            .setCoder(kvFileInfoCoder)
            .apply("Group by Path", GroupByKey.create())
            .apply("Anti-Join Filter", ParDo.of(new OrphanAntiJoinFilterFn(config)))
            .setCoder(fileInfoCoder);

    // Phase 4: Batched Physical Deletion
    PCollection<DeleteOrphanFilesResult> deletionSummary =
        orphanFiles
            .apply(
                "Key for Batching",
                MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptors.strings(), TypeDescriptor.of(FileInfo.class)))
                    .via(
                        file ->
                            KV.of(MoreObjects.firstNonNull(file.getTableIdentifier(), ""), file)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), fileInfoCoder))
            .apply(
                "Batch Files",
                GroupIntoBatches.<String, FileInfo>ofSize(config.deleteBatchSize())
                    .withShardedKey())
            .setCoder(
                KvCoder.of(
                    ShardedKey.Coder.of(StringUtf8Coder.of()), IterableCoder.of(fileInfoCoder)))
            .apply(
                "Delete Orphan Files", ParDo.of(new DeleteOrphanFilesDoFn(catalogConfig, config)))
            .setCoder(resultCoder);

    // Phase 5: Global Metric Aggregation
    return PCollectionList.of(planSummary)
        .and(deletionSummary)
        .apply("Flatten Result Fragments", Flatten.pCollections())
        .setCoder(resultCoder)
        .apply("Merge into Final Result", Combine.globally(new DeleteOrphanFilesResult.Merge()));
  }

  /** Generates a canonical join key combining the table identifier and normalized URI path. */
  public static String makeJoinKey(String tableId, String rawPath) {
    URI uri = new Path(rawPath).toUri();
    return tableId + "#" + uri.getPath();
  }

  /**
   * Flattens map entries where keys may contain comma-separated aliases (e.g. "s3n,s3a" -> "s3").
   */
  public static Map<String, String> flattenMap(Map<String, String> map) {
    Map<String, String> flattened = new HashMap<>();
    if (map != null) {
      for (Map.Entry<String, String> entry : map.entrySet()) {
        String value = entry.getValue().trim();
        for (String splitKey : COMMA_SPLITTER.split(entry.getKey())) {
          flattened.put(splitKey.trim(), value);
        }
      }
    }
    return flattened;
  }

  /** Filters grouped file entries: emits actual files only if no valid metadata entry matches. */
  static class OrphanAntiJoinFilterFn extends DoFn<KV<String, Iterable<FileInfo>>, FileInfo> {
    private final Configuration config;

    OrphanAntiJoinFilterFn(Configuration config) {
      this.config = config;
    }

    @ProcessElement
    public void process(
        @Element KV<String, Iterable<FileInfo>> element, OutputReceiver<FileInfo> out) {
      List<FileInfo> actualEntries = new ArrayList<>();
      List<FileInfo> validEntries = new ArrayList<>();
      Set<String> seenActualPaths = new HashSet<>();

      for (FileInfo info : element.getValue()) {
        if (info.getValid()) {
          validEntries.add(info);
        } else {
          if (seenActualPaths.add(info.getPath())) {
            actualEntries.add(info);
          }
        }
      }

      if (actualEntries.isEmpty()) {
        return;
      }

      if (validEntries.isEmpty()) {
        for (FileInfo actual : actualEntries) {
          out.output(actual);
        }
        return;
      }

      Map<String, String> equalSchemes = config.equalSchemes();
      Map<String, String> equalAuthorities = config.equalAuthorities();
      PrefixMismatchMode mode = config.prefixMismatchMode();

      List<FileURI> validUris = new ArrayList<>();
      for (FileInfo valid : validEntries) {
        URI uri = new Path(valid.getPath()).toUri();
        validUris.add(new FileURI(uri, equalSchemes, equalAuthorities));
      }

      for (FileInfo actual : actualEntries) {
        URI actualRawUri = new Path(actual.getPath()).toUri();
        FileURI actualUri = new FileURI(actualRawUri, equalSchemes, equalAuthorities);

        boolean matched = false;
        String lastValidScheme = null;
        String lastValidAuthority = null;

        for (FileURI validUri : validUris) {
          boolean sMatch = validUri.schemeMatch(actualUri);
          boolean aMatch = validUri.authorityMatch(actualUri);
          if (sMatch && aMatch) {
            matched = true;
            break;
          }
          if (!sMatch) {
            lastValidScheme = validUri.getScheme();
          }
          if (!aMatch) {
            lastValidAuthority = validUri.getAuthority();
          }
        }

        if (matched) {
          continue;
        }

        if (mode == PrefixMismatchMode.DELETE) {
          out.output(actual);
        } else if (mode == PrefixMismatchMode.IGNORE) {
          // Skip
        } else { // ERROR
          throw new ValidationException(
              "Unable to determine whether certain files are orphan. Metadata references files that"
                  + " match listed/provided files except for authority/scheme. Path: %s. Conflicting scheme: [%s vs %s], conflicting authority: [%s vs %s]. Configure equalSchemes/equalAuthorities or change PrefixMismatchMode.",
              actual.getPath(),
              String.valueOf(lastValidScheme),
              String.valueOf(actualUri.getScheme()),
              String.valueOf(lastValidAuthority),
              String.valueOf(actualUri.getAuthority()));
        }
      }
    }
  }

  /** Configuration options for {@link DeleteOrphanFiles}. */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Configuration implements Serializable {

    public static Builder builder() {
      return new AutoValue_DeleteOrphanFiles_Configuration.Builder()
          .setCleanFiles(true)
          .setOlderThanTimestamp(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3))
          .setDeleteBatchSize(10_000)
          .setPrefixMismatchMode(PrefixMismatchMode.ERROR.name())
          .setUsePrefixListing(false)
          .setSubDirectoryListingDepth(3)
          .setSubDirectoryDirectSubDirs(10);
    }

    @SchemaFieldDescription(
        "Custom storage location to scan. If null, defaults to table root location.")
    @Pure
    public abstract @Nullable String getLocation();

    @SchemaFieldDescription(
        "Cutoff timestamp in millis. Files modified at or after this timestamp are not deleted.")
    @Pure
    public abstract @Nullable Long getOlderThanTimestamp();

    @SchemaFieldDescription(
        "Whether to physically delete orphan files from storage. If false, acts as a dry run.")
    @Pure
    public abstract @Nullable Boolean getCleanFiles();

    @SchemaFieldDescription("Batch size for bulk file deletion calls. Default is 10,000.")
    @Pure
    public abstract @Nullable Integer getDeleteBatchSize();

    @SchemaFieldDescription(
        "Mode for handling prefix (scheme/authority) mismatches: ERROR, IGNORE, DELETE.")
    @Pure
    public abstract @Nullable String getPrefixMismatchMode();

    @SchemaFieldDescription("Map of equal schemes (e.g. s3a,s3n -> s3).")
    @Pure
    public abstract @Nullable Map<String, String> getEqualSchemes();

    @SchemaFieldDescription("Map of equal authorities.")
    @Pure
    public abstract @Nullable Map<String, String> getEqualAuthorities();

    @SchemaFieldDescription(
        "Whether to use fast prefix listing on object stores supporting SupportsPrefixOperations.")
    @Pure
    public abstract @Nullable Boolean getUsePrefixListing();

    @SchemaFieldDescription(
        "Maximum shallow directory depth during initial driver/worker planning.")
    @Pure
    public abstract @Nullable Integer getSubDirectoryListingDepth();

    @SchemaFieldDescription(
        "Maximum direct subdirectories before fanning out to worker parallel listing.")
    @Pure
    public abstract @Nullable Integer getSubDirectoryDirectSubDirs();

    public @Nullable String location() {
      return getLocation();
    }

    public long olderThanTimestamp() {
      return MoreObjects.firstNonNull(
          getOlderThanTimestamp(), System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3));
    }

    public boolean cleanFiles() {
      return MoreObjects.firstNonNull(getCleanFiles(), true);
    }

    public int deleteBatchSize() {
      return MoreObjects.firstNonNull(getDeleteBatchSize(), 10_000);
    }

    public PrefixMismatchMode prefixMismatchMode() {
      return PrefixMismatchMode.valueOf(
          MoreObjects.firstNonNull(getPrefixMismatchMode(), PrefixMismatchMode.ERROR.name()));
    }

    public Map<String, String> equalSchemes() {
      Map<String, String> schemes = flattenMap(EQUAL_SCHEMES_DEFAULT);
      if (getEqualSchemes() != null) {
        schemes.putAll(flattenMap(getEqualSchemes()));
      }
      return schemes;
    }

    public Map<String, String> equalAuthorities() {
      if (getEqualAuthorities() != null) {
        return flattenMap(getEqualAuthorities());
      }
      return ImmutableMap.of();
    }

    public boolean usePrefixListing() {
      return MoreObjects.firstNonNull(getUsePrefixListing(), false);
    }

    public int subDirectoryListingDepth() {
      return MoreObjects.firstNonNull(getSubDirectoryListingDepth(), 3);
    }

    public int subDirectoryDirectSubDirs() {
      return MoreObjects.firstNonNull(getSubDirectoryDirectSubDirs(), 10);
    }

    public void validate() {
      if (getOlderThanTimestamp() != null) {
        Preconditions.checkArgument(
            getOlderThanTimestamp() > 0,
            "olderThanTimestamp must be positive, got %s",
            getOlderThanTimestamp());
      }
      if (getDeleteBatchSize() != null) {
        Preconditions.checkArgument(
            getDeleteBatchSize() > 0,
            "deleteBatchSize must be positive, got %s",
            getDeleteBatchSize());
      }
      if (getSubDirectoryListingDepth() != null) {
        Preconditions.checkArgument(
            getSubDirectoryListingDepth() >= 0,
            "subDirectoryListingDepth must be non-negative, got %s",
            getSubDirectoryListingDepth());
      }
      if (getSubDirectoryDirectSubDirs() != null) {
        Preconditions.checkArgument(
            getSubDirectoryDirectSubDirs() > 0,
            "subDirectoryDirectSubDirs must be positive, got %s",
            getSubDirectoryDirectSubDirs());
      }
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setLocation(@Nullable String location);

      public abstract Builder setOlderThanTimestamp(@Nullable Long millis);

      public abstract Builder setCleanFiles(@Nullable Boolean cleanFiles);

      public abstract Builder setDeleteBatchSize(@Nullable Integer size);

      public abstract Builder setPrefixMismatchMode(@Nullable String mode);

      public Builder setPrefixMismatchMode(PrefixMismatchMode mode) {
        return setPrefixMismatchMode(mode != null ? mode.name() : null);
      }

      public abstract Builder setEqualSchemes(@Nullable Map<String, String> schemes);

      public abstract Builder setEqualAuthorities(@Nullable Map<String, String> authorities);

      public abstract Builder setUsePrefixListing(@Nullable Boolean usePrefixListing);

      public abstract Builder setSubDirectoryListingDepth(@Nullable Integer depth);

      public abstract Builder setSubDirectoryDirectSubDirs(@Nullable Integer subDirs);

      public abstract Configuration build();
    }
  }
}
