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
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.beam.sdk.transforms.Combine;

/**
 * Structured summary metrics of a {@link DeleteOrphanFiles} run.
 *
 * <p>All counts default to 0. An empty or no-op run produces an all-zeros result. When {@code
 * cleanFiles} is set to {@code false} (dry run mode), file counts reflect unreferenced orphan
 * candidate files identified for deletion rather than files physically removed from storage.
 */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class DeleteOrphanFilesResult implements Serializable {

  /** Total number of orphan files identified and/or removed from storage. */
  @SchemaFieldNumber("0")
  public abstract long getOrphanFilesCount();

  /** Number of physical data files identified and/or removed from storage. */
  @SchemaFieldNumber("1")
  public abstract long getDeletedDataFilesCount();

  /** Number of position delete files identified and/or removed from storage. */
  @SchemaFieldNumber("2")
  public abstract long getDeletedPositionDeleteFilesCount();

  /** Number of equality delete files identified and/or removed from storage. */
  @SchemaFieldNumber("3")
  public abstract long getDeletedEqualityDeleteFilesCount();

  /** Number of manifest files identified and/or removed from storage. */
  @SchemaFieldNumber("4")
  public abstract long getDeletedManifestsCount();

  /** Number of manifest lists identified and/or removed from storage. */
  @SchemaFieldNumber("5")
  public abstract long getDeletedManifestListsCount();

  /** Number of other metadata files (metadata json, version hints, stats) removed. */
  @SchemaFieldNumber("6")
  public abstract long getDeletedOtherMetadataFilesCount();

  /** A builder with every count pre-set to 0. */
  public static Builder builder() {
    return new AutoValue_DeleteOrphanFilesResult.Builder()
        .setOrphanFilesCount(0L)
        .setDeletedDataFilesCount(0L)
        .setDeletedPositionDeleteFilesCount(0L)
        .setDeletedEqualityDeleteFilesCount(0L)
        .setDeletedManifestsCount(0L)
        .setDeletedManifestListsCount(0L)
        .setDeletedOtherMetadataFilesCount(0L);
  }

  /** The all-zeros identity result. */
  public static DeleteOrphanFilesResult zeros() {
    return builder().build();
  }

  /** Field-wise sum of two result fragments. */
  public static DeleteOrphanFilesResult merge(
      DeleteOrphanFilesResult a, DeleteOrphanFilesResult b) {
    return builder()
        .setOrphanFilesCount(a.getOrphanFilesCount() + b.getOrphanFilesCount())
        .setDeletedDataFilesCount(a.getDeletedDataFilesCount() + b.getDeletedDataFilesCount())
        .setDeletedPositionDeleteFilesCount(
            a.getDeletedPositionDeleteFilesCount() + b.getDeletedPositionDeleteFilesCount())
        .setDeletedEqualityDeleteFilesCount(
            a.getDeletedEqualityDeleteFilesCount() + b.getDeletedEqualityDeleteFilesCount())
        .setDeletedManifestsCount(a.getDeletedManifestsCount() + b.getDeletedManifestsCount())
        .setDeletedManifestListsCount(
            a.getDeletedManifestListsCount() + b.getDeletedManifestListsCount())
        .setDeletedOtherMetadataFilesCount(
            a.getDeletedOtherMetadataFilesCount() + b.getDeletedOtherMetadataFilesCount())
        .build();
  }

  /**
   * Sums per-stage or per-worker result fragments into a single final result. The identity is
   * {@link #zeros()}, so {@code Combine.globally} in a bounded global window emits one all-zeros
   * row even on empty input.
   */
  public static class Merge
      extends Combine.CombineFn<
          DeleteOrphanFilesResult, DeleteOrphanFilesResult, DeleteOrphanFilesResult> {
    @Override
    public DeleteOrphanFilesResult createAccumulator() {
      return zeros();
    }

    @Override
    public DeleteOrphanFilesResult addInput(
        DeleteOrphanFilesResult accumulator, DeleteOrphanFilesResult input) {
      return merge(accumulator, input);
    }

    @Override
    public DeleteOrphanFilesResult mergeAccumulators(
        Iterable<DeleteOrphanFilesResult> accumulators) {
      DeleteOrphanFilesResult merged = zeros();
      for (DeleteOrphanFilesResult acc : accumulators) {
        merged = merge(merged, acc);
      }
      return merged;
    }

    @Override
    public DeleteOrphanFilesResult extractOutput(DeleteOrphanFilesResult accumulator) {
      return accumulator;
    }
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setOrphanFilesCount(long count);

    public abstract Builder setDeletedDataFilesCount(long count);

    public abstract Builder setDeletedPositionDeleteFilesCount(long count);

    public abstract Builder setDeletedEqualityDeleteFilesCount(long count);

    public abstract Builder setDeletedManifestsCount(long count);

    public abstract Builder setDeletedManifestListsCount(long count);

    public abstract Builder setDeletedOtherMetadataFilesCount(long count);

    public abstract DeleteOrphanFilesResult build();
  }
}
