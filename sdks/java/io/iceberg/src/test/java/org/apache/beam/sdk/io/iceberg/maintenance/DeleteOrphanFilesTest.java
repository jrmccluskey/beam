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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles.PrefixMismatchMode;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DeleteOrphanFilesTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private IcebergCatalogConfig getCatalogConfig() {
    return IcebergCatalogConfig.builder()
        .setCatalogProperties(ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
        .build();
  }

  @Test
  public void testStandardOrphanDataFileDeleted() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "standard_orphan_" + System.nanoTime());
    Table table = DeleteOrphanFilesTestFixtures.createTableWithCommittedData(warehouse, tableId);

    long now = System.currentTimeMillis();
    long oldTime = now - 100_000L;
    long cutoff = now - 50_000L;

    // Orphan file older than cutoff
    File orphanFile =
        DeleteOrphanFilesTestFixtures.createOrphanDataFile(
            table, "orphan_data_" + System.nanoTime() + ".parquet", oldTime);
    assertTrue("Orphan file must exist initially", orphanFile.exists());

    // Record committed file path
    Snapshot currentSnapshot = table.currentSnapshot();
    String committedFilePath = getFirstDataFilePath(table, currentSnapshot);
    assertTrue("Committed file must exist initially", fileExists(committedFilePath));

    DeleteOrphanFiles.Configuration config =
        DeleteOrphanFiles.Configuration.builder()
            .setOlderThanTimestamp(cutoff)
            .setCleanFiles(true)
            .build();

    PCollection<DeleteOrphanFilesResult> result =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(DeleteOrphanFiles.create(getCatalogConfig(), config));

    PAssert.that(result)
        .satisfies(
            results -> {
              DeleteOrphanFilesResult r = results.iterator().next();
              assertEquals(1L, r.getOrphanFilesCount());
              assertEquals(1L, r.getDeletedDataFilesCount());
              assertEquals(0L, r.getDeletedManifestsCount());
              return null;
            });

    pipeline.run();

    // Verify orphan file is physically deleted
    assertFalse("Orphan file must be deleted from storage", orphanFile.exists());

    // Verify committed file and table metadata are untouched
    assertTrue("Committed file must still exist", fileExists(committedFilePath));
    table.refresh();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      assertEquals(1, Lists.newArrayList(tasks).size());
    }
  }

  @Test
  public void testRecentInFlightFilesPreserved() throws IOException {
    TableIdentifier tableId =
        TableIdentifier.of("default", "recent_in_flight_" + System.nanoTime());
    Table table = DeleteOrphanFilesTestFixtures.createTableWithCommittedData(warehouse, tableId);

    long now = System.currentTimeMillis();
    long cutoff = now - 100_000L;

    // Orphan file newer than cutoff (created "now")
    File recentOrphanFile =
        DeleteOrphanFilesTestFixtures.createOrphanDataFile(
            table, "recent_orphan_" + System.nanoTime() + ".parquet", now);
    assertTrue("Recent orphan file must exist", recentOrphanFile.exists());

    DeleteOrphanFiles.Configuration config =
        DeleteOrphanFiles.Configuration.builder()
            .setOlderThanTimestamp(cutoff)
            .setCleanFiles(true)
            .build();

    PCollection<DeleteOrphanFilesResult> result =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(DeleteOrphanFiles.create(getCatalogConfig(), config));

    PAssert.that(result).containsInAnyOrder(DeleteOrphanFilesResult.zeros());

    pipeline.run();

    assertTrue(
        "Recent orphan file must NOT be deleted (in-flight protection)", recentOrphanFile.exists());
  }

  @Test
  public void testActiveTableFilesNeverDeleted() throws IOException {
    TableIdentifier tableId =
        TableIdentifier.of("default", "active_table_files_" + System.nanoTime());
    Table table = DeleteOrphanFilesTestFixtures.createTableWithCommittedData(warehouse, tableId);

    // Commit a second snapshot
    DataFile secondDataFile =
        warehouse.writeRecords(
            "second_file_" + System.nanoTime() + ".parquet",
            table.schema(),
            Collections.singletonList(DeleteOrphanFilesTestFixtures.createRecord(2L, "val-2")));
    table.newAppend().appendFile(secondDataFile).commit();
    table.refresh();

    long now = System.currentTimeMillis();
    long oldTime = now - 100_000L;
    long cutoff = now - 50_000L;

    // Create an orphan file
    File orphanFile =
        DeleteOrphanFilesTestFixtures.createOrphanDataFile(
            table, "orphan_to_delete.parquet", oldTime);
    assertTrue(orphanFile.exists());

    DeleteOrphanFiles.Configuration config =
        DeleteOrphanFiles.Configuration.builder()
            .setOlderThanTimestamp(cutoff)
            .setCleanFiles(true)
            .build();

    PCollection<DeleteOrphanFilesResult> result =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(DeleteOrphanFiles.create(getCatalogConfig(), config));

    PAssert.that(result)
        .satisfies(
            results -> {
              DeleteOrphanFilesResult r = results.iterator().next();
              assertEquals(1L, r.getOrphanFilesCount());
              assertEquals(1L, r.getDeletedDataFilesCount());
              return null;
            });

    pipeline.run();

    assertFalse("Orphan file must be deleted", orphanFile.exists());

    // Verify all active data files and manifests are preserved
    table.refresh();
    for (Snapshot snapshot : table.snapshots()) {
      for (ManifestFile manifest : snapshot.allManifests(table.io())) {
        assertTrue("Active manifest must exist: " + manifest.path(), fileExists(manifest.path()));
      }
    }
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      List<FileScanTask> taskList = Lists.newArrayList(tasks);
      assertEquals(2, taskList.size());
      for (FileScanTask task : taskList) {
        assertTrue(
            "Active data file must exist: " + task.file().path(),
            fileExists(task.file().path().toString()));
      }
    }
  }

  @Test
  public void testOrphanManifestAndMetadataFilesPruned() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "orphan_metadata_" + System.nanoTime());
    Table table = DeleteOrphanFilesTestFixtures.createTableWithCommittedData(warehouse, tableId);

    long now = System.currentTimeMillis();
    long oldTime = now - 100_000L;
    long cutoff = now - 50_000L;

    // Create an unreferenced manifest and metadata file in metadata directory
    File orphanManifest =
        DeleteOrphanFilesTestFixtures.createOrphanManifestFile(
            table, "orphan-manifest.avro", oldTime);
    File orphanMetadata =
        DeleteOrphanFilesTestFixtures.createOrphanMetadataFile(
            table, "orphan-v999.metadata.json", oldTime);

    assertTrue(orphanManifest.exists());
    assertTrue(orphanMetadata.exists());

    DeleteOrphanFiles.Configuration config =
        DeleteOrphanFiles.Configuration.builder()
            .setOlderThanTimestamp(cutoff)
            .setCleanFiles(true)
            .build();

    PCollection<DeleteOrphanFilesResult> result =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(DeleteOrphanFiles.create(getCatalogConfig(), config));

    PAssert.that(result)
        .satisfies(
            results -> {
              DeleteOrphanFilesResult r = results.iterator().next();
              assertEquals(2L, r.getOrphanFilesCount());
              assertEquals(1L, r.getDeletedManifestsCount());
              assertEquals(1L, r.getDeletedOtherMetadataFilesCount());
              return null;
            });

    pipeline.run();

    assertFalse("Orphan manifest must be deleted", orphanManifest.exists());
    assertFalse("Orphan metadata json must be deleted", orphanMetadata.exists());
  }

  @Test
  public void testDryRunMode() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "dry_run_" + System.nanoTime());
    Table table = DeleteOrphanFilesTestFixtures.createTableWithCommittedData(warehouse, tableId);

    long now = System.currentTimeMillis();
    long oldTime = now - 100_000L;
    long cutoff = now - 50_000L;

    File orphanFile =
        DeleteOrphanFilesTestFixtures.createOrphanDataFile(
            table, "dry_run_orphan.parquet", oldTime);
    assertTrue(orphanFile.exists());

    DeleteOrphanFiles.Configuration config =
        DeleteOrphanFiles.Configuration.builder()
            .setOlderThanTimestamp(cutoff)
            .setCleanFiles(false)
            .build();

    PCollection<DeleteOrphanFilesResult> result =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(DeleteOrphanFiles.create(getCatalogConfig(), config));

    PAssert.that(result)
        .satisfies(
            results -> {
              DeleteOrphanFilesResult r = results.iterator().next();
              assertEquals(1L, r.getOrphanFilesCount());
              assertEquals(1L, r.getDeletedDataFilesCount());
              return null;
            });

    pipeline.run();

    assertTrue("Orphan file must NOT be deleted in dry run mode", orphanFile.exists());
  }

  @Test
  public void testPrefixMismatchErrorModeThrowsException() throws NoSuchSchemaException {
    String tableId = "default.prefix_error";
    String path = "/warehouse/default/prefix_error/data/file1.parquet";
    String joinKey = tableId + "#" + path;

    FileInfo validFile = FileInfo.of("file://" + path, FileCategory.DATA, true, tableId);
    FileInfo actualFile = FileInfo.of("hdfs://namenode" + path, FileCategory.DATA, false, tableId);

    DeleteOrphanFiles.Configuration config =
        DeleteOrphanFiles.Configuration.builder()
            .setPrefixMismatchMode(PrefixMismatchMode.ERROR)
            .build();

    SchemaCoder<FileInfo> fileInfoCoder =
        pipeline.getSchemaRegistry().getSchemaCoder(FileInfo.class);

    pipeline
        .apply(
            Create.of(KV.of(joinKey, (Iterable<FileInfo>) Arrays.asList(validFile, actualFile)))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(fileInfoCoder))))
        .apply(ParDo.of(new DeleteOrphanFiles.OrphanAntiJoinFilterFn(config)));

    assertThrows(
        Exception.class,
        () -> {
          pipeline.run();
        });
  }

  @Test
  public void testPrefixMismatchIgnoreMode() throws NoSuchSchemaException {
    String tableId = "default.prefix_ignore";
    String path = "/warehouse/default/prefix_ignore/data/file1.parquet";
    String joinKey = tableId + "#" + path;

    FileInfo validFile = FileInfo.of("file://" + path, FileCategory.DATA, true, tableId);
    FileInfo actualFile = FileInfo.of("hdfs://namenode" + path, FileCategory.DATA, false, tableId);

    DeleteOrphanFiles.Configuration config =
        DeleteOrphanFiles.Configuration.builder()
            .setPrefixMismatchMode(PrefixMismatchMode.IGNORE)
            .build();

    SchemaCoder<FileInfo> fileInfoCoder =
        pipeline.getSchemaRegistry().getSchemaCoder(FileInfo.class);

    PCollection<FileInfo> output =
        pipeline
            .apply(
                Create.of(KV.of(joinKey, (Iterable<FileInfo>) Arrays.asList(validFile, actualFile)))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(fileInfoCoder))))
            .apply(ParDo.of(new DeleteOrphanFiles.OrphanAntiJoinFilterFn(config)));

    PAssert.that(output).empty();

    pipeline.run();
  }

  @Test
  public void testPrefixMismatchDeleteMode() throws NoSuchSchemaException {
    String tableId = "default.prefix_delete";
    String path = "/warehouse/default/prefix_delete/data/file1.parquet";
    String joinKey = tableId + "#" + path;

    FileInfo validFile = FileInfo.of("file://" + path, FileCategory.DATA, true, tableId);
    FileInfo actualFile = FileInfo.of("hdfs://namenode" + path, FileCategory.DATA, false, tableId);

    DeleteOrphanFiles.Configuration config =
        DeleteOrphanFiles.Configuration.builder()
            .setPrefixMismatchMode(PrefixMismatchMode.DELETE)
            .build();

    SchemaCoder<FileInfo> fileInfoCoder =
        pipeline.getSchemaRegistry().getSchemaCoder(FileInfo.class);

    PCollection<FileInfo> output =
        pipeline
            .apply(
                Create.of(KV.of(joinKey, (Iterable<FileInfo>) Arrays.asList(validFile, actualFile)))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(fileInfoCoder))))
            .apply(ParDo.of(new DeleteOrphanFiles.OrphanAntiJoinFilterFn(config)));

    PAssert.that(output).containsInAnyOrder(actualFile);

    pipeline.run();
  }

  @Test
  public void testPrefixMismatchResolvedByEqualSchemes() throws NoSuchSchemaException {
    String tableId = "default.prefix_equal";
    String path = "/warehouse/default/prefix_equal/data/file1.parquet";
    String joinKey = tableId + "#" + path;

    FileInfo validFile = FileInfo.of("s3://bucket" + path, FileCategory.DATA, true, tableId);
    FileInfo actualFile = FileInfo.of("s3a://bucket" + path, FileCategory.DATA, false, tableId);

    // Default config maps s3a,s3n -> s3
    DeleteOrphanFiles.Configuration config =
        DeleteOrphanFiles.Configuration.builder()
            .setPrefixMismatchMode(PrefixMismatchMode.ERROR)
            .build();

    SchemaCoder<FileInfo> fileInfoCoder =
        pipeline.getSchemaRegistry().getSchemaCoder(FileInfo.class);

    PCollection<FileInfo> output =
        pipeline
            .apply(
                Create.of(KV.of(joinKey, (Iterable<FileInfo>) Arrays.asList(validFile, actualFile)))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(fileInfoCoder))))
            .apply(ParDo.of(new DeleteOrphanFiles.OrphanAntiJoinFilterFn(config)));

    // Since schemes are mapped to equal, matched = true, so file is valid (not orphan)
    PAssert.that(output).empty();

    pipeline.run();
  }

  @Test
  public void testCustomScanLocation() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "custom_location_" + System.nanoTime());
    Table table = DeleteOrphanFilesTestFixtures.createTableWithCommittedData(warehouse, tableId);

    long now = System.currentTimeMillis();
    long oldTime = now - 100_000L;
    long cutoff = now - 50_000L;

    File tableDir = new File(table.location().replaceFirst("^file:", ""));
    File targetSubDir = new File(tableDir, "targeted_subfolder");
    assertTrue(targetSubDir.mkdirs());

    File orphanInTarget =
        DeleteOrphanFilesTestFixtures.createOrphanFile(
            targetSubDir, "target_orphan.parquet", oldTime, "dummy");
    File orphanOutsideTarget =
        DeleteOrphanFilesTestFixtures.createOrphanDataFile(
            table, "outside_orphan.parquet", oldTime);

    assertTrue(orphanInTarget.exists());
    assertTrue(orphanOutsideTarget.exists());

    // Scan only the targeted subfolder
    DeleteOrphanFiles.Configuration config =
        DeleteOrphanFiles.Configuration.builder()
            .setLocation(targetSubDir.toURI().toString())
            .setOlderThanTimestamp(cutoff)
            .setCleanFiles(true)
            .build();

    PCollection<DeleteOrphanFilesResult> result =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(DeleteOrphanFiles.create(getCatalogConfig(), config));

    PAssert.that(result)
        .satisfies(
            results -> {
              DeleteOrphanFilesResult r = results.iterator().next();
              assertEquals(1L, r.getOrphanFilesCount());
              assertEquals(1L, r.getDeletedDataFilesCount());
              return null;
            });

    pipeline.run();

    assertFalse("Target orphan file must be deleted", orphanInTarget.exists());
    assertTrue(
        "Orphan file outside custom scan location must be preserved", orphanOutsideTarget.exists());
  }

  @Test
  public void testMultipleTables() throws IOException {
    TableIdentifier tableId1 = TableIdentifier.of("default", "multi_table_1_" + System.nanoTime());
    TableIdentifier tableId2 = TableIdentifier.of("default", "multi_table_2_" + System.nanoTime());

    Table table1 = DeleteOrphanFilesTestFixtures.createTableWithCommittedData(warehouse, tableId1);
    Table table2 = DeleteOrphanFilesTestFixtures.createTableWithCommittedData(warehouse, tableId2);

    long now = System.currentTimeMillis();
    long oldTime = now - 100_000L;
    long cutoff = now - 50_000L;

    File orphan1 =
        DeleteOrphanFilesTestFixtures.createOrphanDataFile(table1, "orphan1.parquet", oldTime);
    File orphan2 =
        DeleteOrphanFilesTestFixtures.createOrphanDataFile(table2, "orphan2.parquet", oldTime);
    File orphan3 =
        DeleteOrphanFilesTestFixtures.createOrphanDataFile(table2, "orphan3.parquet", oldTime);

    assertTrue(orphan1.exists());
    assertTrue(orphan2.exists());
    assertTrue(orphan3.exists());

    DeleteOrphanFiles.Configuration config =
        DeleteOrphanFiles.Configuration.builder()
            .setOlderThanTimestamp(cutoff)
            .setCleanFiles(true)
            .build();

    PCollection<DeleteOrphanFilesResult> result =
        pipeline
            .apply(Create.of(tableId1.toString(), tableId2.toString()))
            .apply(DeleteOrphanFiles.create(getCatalogConfig(), config));

    PAssert.that(result)
        .satisfies(
            results -> {
              DeleteOrphanFilesResult r = results.iterator().next();
              assertEquals(3L, r.getOrphanFilesCount());
              assertEquals(3L, r.getDeletedDataFilesCount());
              return null;
            });

    pipeline.run();

    assertFalse(orphan1.exists());
    assertFalse(orphan2.exists());
    assertFalse(orphan3.exists());
  }

  @Test
  public void testEmptyInputProducesEmptyOutput() {
    DeleteOrphanFiles.Configuration config = DeleteOrphanFiles.Configuration.builder().build();

    PCollection<DeleteOrphanFilesResult> result =
        pipeline
            .apply(Create.empty(StringUtf8Coder.of()))
            .apply(DeleteOrphanFiles.create(getCatalogConfig(), config));

    PAssert.that(result).containsInAnyOrder(DeleteOrphanFilesResult.zeros());

    pipeline.run();
  }

  @Test
  public void testTableWithZeroSnapshotsPreservesMetadata() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "zero_snapshots_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    long now = System.currentTimeMillis();
    long oldTime = now - 100_000L;
    long cutoff = now - 50_000L;

    File orphan =
        DeleteOrphanFilesTestFixtures.createOrphanDataFile(table, "orphan.parquet", oldTime);
    assertTrue(orphan.exists());

    DeleteOrphanFiles.Configuration config =
        DeleteOrphanFiles.Configuration.builder()
            .setOlderThanTimestamp(cutoff)
            .setCleanFiles(true)
            .build();

    PCollection<DeleteOrphanFilesResult> result =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(DeleteOrphanFiles.create(getCatalogConfig(), config));

    PAssert.that(result)
        .satisfies(
            results -> {
              DeleteOrphanFilesResult r = results.iterator().next();
              assertEquals(1L, r.getOrphanFilesCount());
              assertEquals(1L, r.getDeletedDataFilesCount());
              return null;
            });

    pipeline.run();

    assertFalse(orphan.exists());
    table.refresh();
    assertEquals(0, Lists.newArrayList(table.snapshots()).size());
  }

  @Test
  public void testOrphanEqualityDeleteFilesPruned() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "eq_deletes_" + System.nanoTime());
    Table table = DeleteOrphanFilesTestFixtures.createTableWithCommittedData(warehouse, tableId);

    long now = System.currentTimeMillis();
    long oldTime = now - 100_000L;
    long cutoff = now - 50_000L;

    File orphanEqDel =
        DeleteOrphanFilesTestFixtures.createOrphanDataFile(
            table, "orphan-equality-delete.parquet", oldTime);
    assertTrue(orphanEqDel.exists());

    DeleteOrphanFiles.Configuration config =
        DeleteOrphanFiles.Configuration.builder()
            .setOlderThanTimestamp(cutoff)
            .setCleanFiles(true)
            .build();

    PCollection<DeleteOrphanFilesResult> result =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(DeleteOrphanFiles.create(getCatalogConfig(), config));

    PAssert.that(result)
        .satisfies(
            results -> {
              DeleteOrphanFilesResult r = results.iterator().next();
              assertEquals(1L, r.getOrphanFilesCount());
              assertEquals(1L, r.getDeletedEqualityDeleteFilesCount());
              return null;
            });

    pipeline.run();

    assertFalse(orphanEqDel.exists());
  }

  @Test
  public void testStatisticsFilesPreserved() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "stats_preserve_" + System.nanoTime());
    Table table = DeleteOrphanFilesTestFixtures.createTableWithCommittedData(warehouse, tableId);

    long now = System.currentTimeMillis();
    long oldTime = now - 100_000L;
    long cutoff = now - 50_000L;

    // Create an unreferenced stats file that should be deleted
    File orphanStats =
        DeleteOrphanFilesTestFixtures.createOrphanFile(
            new File(table.location().replaceFirst("^file:", ""), "metadata"),
            "orphan_stats.puffin",
            oldTime,
            "puffin-bytes");
    assertTrue(orphanStats.exists());

    DeleteOrphanFiles.Configuration config =
        DeleteOrphanFiles.Configuration.builder()
            .setOlderThanTimestamp(cutoff)
            .setCleanFiles(true)
            .build();

    PCollection<DeleteOrphanFilesResult> result =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(DeleteOrphanFiles.create(getCatalogConfig(), config));

    PAssert.that(result)
        .satisfies(
            results -> {
              DeleteOrphanFilesResult r = results.iterator().next();
              assertEquals(1L, r.getOrphanFilesCount());
              assertEquals(1L, r.getDeletedOtherMetadataFilesCount());
              return null;
            });

    pipeline.run();

    assertFalse(orphanStats.exists());
  }

  private static boolean fileExists(String path) {
    return new File(new Path(path).toUri()).exists();
  }

  private static String getFirstDataFilePath(Table table, Snapshot snapshot) throws IOException {
    List<DataFile> files = Lists.newArrayList(snapshot.addedDataFiles(table.io()));
    if (!files.isEmpty()) {
      return files.get(0).path().toString();
    }
    return null;
  }
}
