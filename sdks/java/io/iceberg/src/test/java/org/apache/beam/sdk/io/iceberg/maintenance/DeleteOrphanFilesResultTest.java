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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DeleteOrphanFilesResultTest {

  @Test
  public void testZerosIdentity() {
    DeleteOrphanFilesResult zeros = DeleteOrphanFilesResult.zeros();
    assertEquals(0L, zeros.getOrphanFilesCount());
    assertEquals(0L, zeros.getDeletedDataFilesCount());
    assertEquals(0L, zeros.getDeletedPositionDeleteFilesCount());
    assertEquals(0L, zeros.getDeletedEqualityDeleteFilesCount());
    assertEquals(0L, zeros.getDeletedManifestsCount());
    assertEquals(0L, zeros.getDeletedManifestListsCount());
    assertEquals(0L, zeros.getDeletedOtherMetadataFilesCount());
  }

  @Test
  public void testMergeFragments() {
    DeleteOrphanFilesResult a =
        DeleteOrphanFilesResult.builder()
            .setOrphanFilesCount(12L)
            .setDeletedDataFilesCount(5L)
            .setDeletedPositionDeleteFilesCount(2L)
            .setDeletedEqualityDeleteFilesCount(1L)
            .setDeletedManifestsCount(3L)
            .setDeletedManifestListsCount(1L)
            .setDeletedOtherMetadataFilesCount(0L)
            .build();

    DeleteOrphanFilesResult b =
        DeleteOrphanFilesResult.builder()
            .setOrphanFilesCount(17L)
            .setDeletedDataFilesCount(10L)
            .setDeletedPositionDeleteFilesCount(0L)
            .setDeletedEqualityDeleteFilesCount(3L)
            .setDeletedManifestsCount(2L)
            .setDeletedManifestListsCount(1L)
            .setDeletedOtherMetadataFilesCount(1L)
            .build();

    DeleteOrphanFilesResult merged = DeleteOrphanFilesResult.merge(a, b);
    assertEquals(29L, merged.getOrphanFilesCount());
    assertEquals(15L, merged.getDeletedDataFilesCount());
    assertEquals(2L, merged.getDeletedPositionDeleteFilesCount());
    assertEquals(4L, merged.getDeletedEqualityDeleteFilesCount());
    assertEquals(5L, merged.getDeletedManifestsCount());
    assertEquals(2L, merged.getDeletedManifestListsCount());
    assertEquals(1L, merged.getDeletedOtherMetadataFilesCount());
  }

  @Test
  public void testCombineFn() {
    DeleteOrphanFilesResult.Merge mergeFn = new DeleteOrphanFilesResult.Merge();
    DeleteOrphanFilesResult acc = mergeFn.createAccumulator();
    assertEquals(DeleteOrphanFilesResult.zeros(), acc);

    DeleteOrphanFilesResult item1 =
        DeleteOrphanFilesResult.builder()
            .setOrphanFilesCount(3L)
            .setDeletedDataFilesCount(3L)
            .build();
    DeleteOrphanFilesResult item2 =
        DeleteOrphanFilesResult.builder()
            .setOrphanFilesCount(9L)
            .setDeletedDataFilesCount(7L)
            .setDeletedManifestsCount(2L)
            .build();

    acc = mergeFn.addInput(acc, item1);
    acc = mergeFn.addInput(acc, item2);

    DeleteOrphanFilesResult output = mergeFn.extractOutput(acc);
    assertEquals(12L, output.getOrphanFilesCount());
    assertEquals(10L, output.getDeletedDataFilesCount());
    assertEquals(2L, output.getDeletedManifestsCount());

    DeleteOrphanFilesResult mergedAcc =
        mergeFn.mergeAccumulators(Arrays.asList(item1, item2, DeleteOrphanFilesResult.zeros()));
    assertEquals(12L, mergedAcc.getOrphanFilesCount());
    assertEquals(10L, mergedAcc.getDeletedDataFilesCount());
    assertEquals(2L, mergedAcc.getDeletedManifestsCount());

    DeleteOrphanFilesResult emptyMerge = mergeFn.mergeAccumulators(Collections.emptyList());
    assertEquals(DeleteOrphanFilesResult.zeros(), emptyMerge);
  }

  @Test
  public void testSchemaCoderSerialization() throws NoSuchSchemaException, IOException {
    Coder<DeleteOrphanFilesResult> coder =
        SchemaRegistry.createDefault().getSchemaCoder(DeleteOrphanFilesResult.class);

    DeleteOrphanFilesResult original =
        DeleteOrphanFilesResult.builder()
            .setOrphanFilesCount(282L)
            .setDeletedDataFilesCount(123L)
            .setDeletedPositionDeleteFilesCount(45L)
            .setDeletedEqualityDeleteFilesCount(6L)
            .setDeletedManifestsCount(78L)
            .setDeletedManifestListsCount(9L)
            .setDeletedOtherMetadataFilesCount(21L)
            .build();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    coder.encode(original, out);
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    DeleteOrphanFilesResult decoded = coder.decode(in);

    assertEquals(original, decoded);
  }
}
