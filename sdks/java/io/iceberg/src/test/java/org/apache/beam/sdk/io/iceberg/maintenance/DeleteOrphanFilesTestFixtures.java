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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;

/** Test fixtures and file generators for testing {@link DeleteOrphanFiles}. */
public class DeleteOrphanFilesTestFixtures {

  public static Record createRecord(long id, String data) {
    Record r = GenericRecord.create(TestFixtures.SCHEMA);
    r.setField("id", id);
    r.setField("data", data);
    return r;
  }

  /**
   * Creates an orphan physical file directly in {@code parentDir} with the given name, content, and
   * last modified timestamp, completely detached from table metadata.
   */
  public static File createOrphanFile(
      File parentDir, String fileName, long lastModifiedMillis, String content) throws IOException {
    if (!parentDir.exists()) {
      parentDir.mkdirs();
    }
    File file = new File(parentDir, fileName);
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(content.getBytes(StandardCharsets.UTF_8));
    }
    file.setLastModified(lastModifiedMillis);
    return file;
  }

  /** Creates an orphan physical data file in the table's data folder. */
  public static File createOrphanDataFile(Table table, String fileName, long lastModifiedMillis)
      throws IOException {
    File tableLocation = new File(table.location().replaceFirst("^file:", ""));
    File dataDir = new File(tableLocation, "data");
    return createOrphanFile(dataDir, fileName, lastModifiedMillis, "dummy-orphan-data-bytes");
  }

  /** Creates an orphan manifest file in the table's metadata folder. */
  public static File createOrphanManifestFile(Table table, String fileName, long lastModifiedMillis)
      throws IOException {
    File tableLocation = new File(table.location().replaceFirst("^file:", ""));
    File metadataDir = new File(tableLocation, "metadata");
    return createOrphanFile(metadataDir, fileName, lastModifiedMillis, "dummy-orphan-manifest");
  }

  /** Creates an orphan metadata file in the table's metadata folder. */
  public static File createOrphanMetadataFile(Table table, String fileName, long lastModifiedMillis)
      throws IOException {
    File tableLocation = new File(table.location().replaceFirst("^file:", ""));
    File metadataDir = new File(tableLocation, "metadata");
    return createOrphanFile(metadataDir, fileName, lastModifiedMillis, "{\"format-version\":2}");
  }

  /** Creates a table with a committed snapshot containing a valid data file. */
  public static Table createTableWithCommittedData(
      TestDataWarehouse warehouse, TableIdentifier tableId) throws IOException {
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    DataFile file =
        warehouse.writeRecords(
            "committed_file_" + System.nanoTime() + ".parquet",
            table.schema(),
            Collections.singletonList(createRecord(1L, "val-1")));
    table.newAppend().appendFile(file).commit();
    table.refresh();
    return table;
  }
}
