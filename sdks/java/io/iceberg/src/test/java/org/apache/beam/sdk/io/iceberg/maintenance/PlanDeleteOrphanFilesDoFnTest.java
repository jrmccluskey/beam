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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PlanDeleteOrphanFilesDoFnTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private IcebergCatalogConfig getCatalogConfig() {
    return IcebergCatalogConfig.builder()
        .setCatalogProperties(ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
        .build();
  }

  @Test
  public void testInferFileCategory() {
    assertEquals(
        FileCategory.OTHER_METADATA,
        PlanDeleteOrphanFilesDoFn.inferFileCategory("s3://bucket/table/metadata/v1.metadata.json"));
    assertEquals(
        FileCategory.OTHER_METADATA,
        PlanDeleteOrphanFilesDoFn.inferFileCategory(
            "s3://bucket/table/metadata/version-hint.text"));
    assertEquals(
        FileCategory.STATISTICS,
        PlanDeleteOrphanFilesDoFn.inferFileCategory("s3://bucket/table/metadata/stats.puffin"));
    assertEquals(
        FileCategory.MANIFEST_LIST,
        PlanDeleteOrphanFilesDoFn.inferFileCategory(
            "s3://bucket/table/metadata/snap-12345-1-uuid.avro"));
    assertEquals(
        FileCategory.MANIFEST,
        PlanDeleteOrphanFilesDoFn.inferFileCategory(
            "s3://bucket/table/metadata/00000-1-uuid.avro"));
    assertEquals(
        FileCategory.EQUALITY_DELETES,
        PlanDeleteOrphanFilesDoFn.inferFileCategory(
            "s3://bucket/table/data/00000-1-uuid-equality-delete.parquet"));
    assertEquals(
        FileCategory.POSITION_DELETES,
        PlanDeleteOrphanFilesDoFn.inferFileCategory(
            "s3://bucket/table/data/00000-1-uuid-deletes.parquet"));
    assertEquals(
        FileCategory.DATA,
        PlanDeleteOrphanFilesDoFn.inferFileCategory("s3://bucket/table/data/00000-1-uuid.parquet"));
    assertEquals(
        FileCategory.DATA,
        PlanDeleteOrphanFilesDoFn.inferFileCategory("s3://bucket/table/data/00000-1-uuid.orc"));
    assertEquals(
        FileCategory.UNKNOWN,
        PlanDeleteOrphanFilesDoFn.inferFileCategory("s3://bucket/table/readme.txt"));
    assertEquals(FileCategory.UNKNOWN, PlanDeleteOrphanFilesDoFn.inferFileCategory(null));
  }

  @Test
  public void testPlanThrowsWhenGcDisabled() {
    TableIdentifier tableId = TableIdentifier.of("default", "gc_disabled_" + System.nanoTime());
    warehouse.createTable(
        tableId, TestFixtures.SCHEMA, null, ImmutableMap.of(TableProperties.GC_ENABLED, "false"));

    DeleteOrphanFiles.Configuration config = DeleteOrphanFiles.Configuration.builder().build();

    pipeline
        .apply(Create.of(tableId.toString()))
        .apply(
            ParDo.of(new PlanDeleteOrphanFilesDoFn(getCatalogConfig(), config))
                .withOutputTags(
                    PlanDeleteOrphanFilesDoFn.PLAN_SUMMARY,
                    TupleTagList.of(PlanDeleteOrphanFilesDoFn.METADATA_FILES)
                        .and(PlanDeleteOrphanFilesDoFn.MANIFESTS)
                        .and(PlanDeleteOrphanFilesDoFn.SUB_DIRECTORIES)
                        .and(PlanDeleteOrphanFilesDoFn.STORAGE_FILES)));

    assertThrows(
        Exception.class,
        () -> {
          pipeline.run();
        });
  }

  @Test
  public void testPlanGathersValidMetadataAndIdentifiesOrphanStorageFiles()
      throws IOException, NoSuchSchemaException {
    TableIdentifier tableId = TableIdentifier.of("default", "plan_test_" + System.nanoTime());
    Table table = DeleteOrphanFilesTestFixtures.createTableWithCommittedData(warehouse, tableId);

    long now = System.currentTimeMillis();
    long oldTime = now - 100_000L;
    long cutoff = now - 50_000L;

    // Create an orphan file older than cutoff
    File orphanDataFile =
        DeleteOrphanFilesTestFixtures.createOrphanDataFile(
            table, "orphan_data_" + System.nanoTime() + ".parquet", oldTime);
    assertTrue(orphanDataFile.exists());

    // Create a recent file newer than cutoff
    File recentDataFile =
        DeleteOrphanFilesTestFixtures.createOrphanDataFile(
            table, "recent_data_" + System.nanoTime() + ".parquet", now);
    assertTrue(recentDataFile.exists());

    DeleteOrphanFiles.Configuration config =
        DeleteOrphanFiles.Configuration.builder()
            .setOlderThanTimestamp(cutoff)
            .setCleanFiles(false)
            .build();

    SchemaRegistry schemaRegistry = SchemaRegistry.createDefault();
    SchemaCoder<FileInfo> fileInfoCoder = schemaRegistry.getSchemaCoder(FileInfo.class);

    PCollectionTuple planned =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(
                ParDo.of(new PlanDeleteOrphanFilesDoFn(getCatalogConfig(), config))
                    .withOutputTags(
                        PlanDeleteOrphanFilesDoFn.PLAN_SUMMARY,
                        TupleTagList.of(PlanDeleteOrphanFilesDoFn.METADATA_FILES)
                            .and(PlanDeleteOrphanFilesDoFn.MANIFESTS)
                            .and(PlanDeleteOrphanFilesDoFn.SUB_DIRECTORIES)
                            .and(PlanDeleteOrphanFilesDoFn.STORAGE_FILES)));

    PCollection<DeleteOrphanFilesResult> planSummary =
        planned.get(PlanDeleteOrphanFilesDoFn.PLAN_SUMMARY);
    PAssert.that(planSummary).containsInAnyOrder(DeleteOrphanFilesResult.zeros());

    final String expectedTableId = tableId.toString();

    PCollection<KV<String, ManifestFileBean>> manifests =
        planned
            .get(PlanDeleteOrphanFilesDoFn.MANIFESTS)
            .setCoder(
                KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(ManifestFileBean.class)));
    PAssert.that(manifests)
        .satisfies(
            manifestKvs -> {
              int count = 0;
              for (KV<String, ManifestFileBean> kv : manifestKvs) {
                count++;
                assertEquals(expectedTableId, kv.getKey());
                assertTrue(kv.getValue().isValid());
              }
              assertTrue("Must have at least one manifest", count > 0);
              return null;
            });

    PCollection<KV<String, FileInfo>> metadataFiles =
        planned
            .get(PlanDeleteOrphanFilesDoFn.METADATA_FILES)
            .setCoder(KvCoder.of(StringUtf8Coder.of(), fileInfoCoder));
    PAssert.that(metadataFiles)
        .satisfies(
            files -> {
              boolean foundManifestList = false;
              for (KV<String, FileInfo> kv : files) {
                if (kv.getValue().fileCategory() == FileCategory.MANIFEST_LIST) {
                  foundManifestList = true;
                }
                assertTrue(kv.getValue().getValid());
                assertEquals(expectedTableId, kv.getValue().getTableIdentifier());
              }
              assertTrue("Must discover manifest list", foundManifestList);
              return null;
            });

    pipeline.run();
  }
}
