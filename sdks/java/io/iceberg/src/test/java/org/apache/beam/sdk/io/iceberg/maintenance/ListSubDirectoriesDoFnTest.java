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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ListSubDirectoriesDoFnTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private IcebergCatalogConfig getCatalogConfig() {
    return IcebergCatalogConfig.builder()
        .setCatalogProperties(ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
        .build();
  }

  @Test
  public void testListsFilesRecursivelyAndFiltersByCutoff() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "list_subdir_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    File tableDir = new File(table.location().replaceFirst("^file:", ""));
    File subDir = new File(tableDir, "nested/partition_1");
    assertTrue(subDir.mkdirs());

    long now = System.currentTimeMillis();
    long oldTime = now - 100_000L;
    long cutoff = now - 50_000L;

    File oldFile =
        DeleteOrphanFilesTestFixtures.createOrphanFile(
            subDir, "old_file.parquet", oldTime, "dummy-old");
    File recentFile =
        DeleteOrphanFilesTestFixtures.createOrphanFile(
            subDir, "recent_file.parquet", now, "dummy-recent");
    assertTrue(oldFile.exists());
    assertTrue(recentFile.exists());

    DeleteOrphanFiles.Configuration config =
        DeleteOrphanFiles.Configuration.builder().setOlderThanTimestamp(cutoff).build();

    PCollection<KV<String, FileInfo>> output =
        pipeline
            .apply(Create.of(KV.of(tableId.toString(), subDir.toURI().toString())))
            .apply(ParDo.of(new ListSubDirectoriesDoFn(getCatalogConfig(), config)));

    final String expectedTableId = tableId.toString();
    PAssert.that(output)
        .satisfies(
            elements -> {
              int count = 0;
              for (KV<String, FileInfo> kv : elements) {
                count++;
                FileInfo info = kv.getValue();
                assertEquals(expectedTableId, info.getTableIdentifier());
                assertFalse(info.getValid());
                assertEquals(FileCategory.DATA, info.fileCategory());
                assertTrue(info.getPath().endsWith("old_file.parquet"));
              }
              assertEquals("Only old file should be listed", 1, count);
              return null;
            });

    pipeline.run();
  }
}
