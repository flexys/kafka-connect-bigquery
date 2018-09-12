package com.wepay.kafka.connect.bigquery;

/*
 * Copyright 2016 WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.StorageException;

import com.wepay.kafka.connect.bigquery.write.row.GCSToBQWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A Runnable that runs a GCS to BQ Load task.
 *
 * <p>This task goes through the given GCS bucket, and takes as many blobs as a single load job per
 * table can handle (as defined here: https://cloud.google.com/bigquery/quotas#load_jobs) and runs
 * those load jobs. Blobs are deleted (only) once a load job involving that blob succeeds.
 */
public class GCSToBQLoadRunnable implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(GCSToBQLoadRunnable.class);

  private final BigQuery bigQuery;
  private final Bucket bucket;
  private final Map<Job, List<Blob>> activeJobs;
  private final Set<Blob> claimedBlobs;
  private final Set<Blob> deletableBlobs;

  // these numbers are intended to try to make this task not excede Google Cloud Quotas.
  // see: https://cloud.google.com/bigquery/quotas#load_jobs

  // max number of files we can load in a single load job
  private static int FILE_LOAD_LIMIT = 10000;
  // max total size (in bytes) of the files we can load in a single load job.
  private static long MAX_LOAD_SIZE_B = 15 * 1000000000000L; // 15TB

  private static String SOURCE_URI_FORMAT = "gs://%s/%s";
  public static final Pattern METADATA_TABLE_PATTERN =
          Pattern.compile("(?<project>[^:]+):(?<dataset>[^.]+).(?<table>.+)");

  /**
   * Create a {@link GCSToBQLoadRunnable} with the given bigquery, bucket, and ms wait interval.
   * @param bigQuery the {@link BigQuery} instance.
   * @param bucket the the GCS bucket to read from.
   */
  public GCSToBQLoadRunnable(BigQuery bigQuery, Bucket bucket) {
    this.bigQuery = bigQuery;
    this.bucket = bucket;
    this.activeJobs = new HashMap<>();
    this.claimedBlobs = new HashSet<>();
    this.deletableBlobs = new HashSet<>();
  }

  /**
   * Return a map of {@link TableId}s to a list of {@link Blob}s intended to be batch-loaded into
   * that table.
   *
   * <p>Each blob list will not exceed the {@link #FILE_LOAD_LIMIT} in number of blobs or
   * {@link #MAX_LOAD_SIZE_B} in total byte size. Blobs that are already claimed by an in-progress
   * load job will also not be included.
   * @return map from {@link TableId}s to {@link Blob}s.
   */
  private Map<TableId, List<Blob>> getBlobsUpToLimit() {
    Map<TableId, List<Blob>> tableToURIs = new HashMap<>();
    Map<TableId, Long> tableToCurrentLoadSize = new HashMap<>();

    Page<Blob> list = bucket.list();
    for (Blob blob : list.iterateAll()) {
      TableId table = getTableFromBlob(blob);

      if (table == null || claimedBlobs.contains(blob) || deletableBlobs.contains(blob)) {
        // don't do anything if:
        // 1. we don't know what table this should be uploaded to or
        // 2. this blob is already claimed by a currently-running job or
        // 3. this blob is up for deletion.
        continue;
      }

      if (!tableToURIs.containsKey(table)) {
        // initialize maps, if we haven't seen this table before.
        tableToURIs.put(table, new ArrayList<>());
        tableToCurrentLoadSize.put(table, 0L);
      }

      long newSize = tableToCurrentLoadSize.get(table) + blob.getSize();
      // if this file does not cause us to exceed our per-request quota limits...
      if (newSize < MAX_LOAD_SIZE_B && tableToURIs.get(table).size() < FILE_LOAD_LIMIT) {
        // ...add the file (and update the load size)
        tableToURIs.get(table).add(blob);
        tableToCurrentLoadSize.put(table, newSize);
      }
    }
    return tableToURIs;
  }

  /**
   * Given a blob, return the {@link TableId} this blob should be inserted into.
   * @param blob the blob
   * @return the TableId this data should be loaded into, or null if we could not tell what
   *         table it should be loaded into.
   */
  private TableId getTableFromBlob(Blob blob) {
    if (blob.getMetadata() == null
        || blob.getMetadata().get(GCSToBQWriter.GCS_METADATA_TABLE_KEY) == null) {
      logger.error("Found blob {}\\{} with no metadata.", blob.getBucket(), blob.getName());
      return null;
    }

    String serializedTableId = blob.getMetadata().get(GCSToBQWriter.GCS_METADATA_TABLE_KEY);
    Matcher matcher = METADATA_TABLE_PATTERN.matcher(serializedTableId);
    String project = matcher.group("project");
    String dataset = matcher.group("dataset");
    String table = matcher.group("table");
    logger.debug("Table data: project: {}; dataset: {}; table: {}", project, dataset, table);

    if (project == null || dataset == null || table == null) {
      logger.error("Found blob `{}/{}` without parsable table metadata.",
                   blob.getBucket(), blob.getName());
      return null;
    }

    return TableId.of(project, dataset, table);
  }

  /**
   * Trigger a BigQuery load job for each table in the input containing all the blobs associated
   * with that table.
   * @param tablesToBlobs a map of {@link TableId} to the list of {@link Blob}s to be loaded into
   *                      that table.
   * @return a map from Jobs to the list of blobs being loaded in that job.
   */
  private Map<Job, List<Blob>> triggerBigQueryLoadJobs(Map<TableId, List<Blob>> tablesToBlobs) {
    Map<Job, List<Blob>> newJobs = new HashMap<>(tablesToBlobs.size());
    for (Map.Entry<TableId, List<Blob>> entry : tablesToBlobs.entrySet()) {
      newJobs.put(triggerBigQueryLoadJob(entry.getKey(), entry.getValue()), entry.getValue());
    }
    return newJobs;
  }

  private Job triggerBigQueryLoadJob(TableId table, List<Blob> blobs) {
    List<String> uris = blobs.stream()
                             .map(b -> String.format(SOURCE_URI_FORMAT,
                                                     bucket.getName(),
                                                     b.getName()))
                             .collect(Collectors.toList());
    // create job load configuration
    LoadJobConfiguration loadJobConfiguration =
        LoadJobConfiguration.newBuilder(table, uris)
            .setFormatOptions(FormatOptions.json())
            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .build();
    // create and return the job.
    Job job = bigQuery.create(JobInfo.of(loadJobConfiguration));
    // update active jobs and claimed blobs.
    activeJobs.put(job, blobs);
    claimedBlobs.addAll(blobs);
    logger.info("Triggered load job for table {} with {} blobs.", table, blobs.size());
    return job;
  }

  /**
   * Check all active jobs. Remove those that have completed successfully and log a message for
   * any jobs that failed. We only log a message for failed jobs because those blobs will be
   * retried during the next run.
   */
  private void checkJobs() {
    if (activeJobs.isEmpty()) {
      // quick exit if nothing needs to be done.
      return;
    }
    Iterator<Job> jobIterator = activeJobs.keySet().iterator();
    int successCount = 0;
    int failureCount = 0;
    while (jobIterator.hasNext()) {
      Job job = jobIterator.next();
      try {
        if (job.isDone()) {
          List<Blob> blobsToDelete = activeJobs.remove(job);
          successCount++;
          claimedBlobs.removeAll(blobsToDelete);
          deletableBlobs.addAll(blobsToDelete);
        }
      } catch (BigQueryException ex) {
        // log a message.
        logger.warn("GCS to BQ load job failed", ex);
        // remove job from active jobs (it's not active anymore)
        activeJobs.remove(job);
        failureCount++;
      } finally {
        logger.info("GCS To BQ job tally: {} successful jobs, {} failed jobs.",
                    successCount, failureCount);
      }
    }
  }

  /**
   * Delete deletable blobs.
   */
  private void deleteBlobs() {
    int deletedBlobs = 0;
    for (Blob blob : deletableBlobs) {
      try {
        blob.delete();
        deletableBlobs.remove(blob);
      } catch (StorageException ex) {
        logger.warn("Failed to delete blob: {}/{}", blob.getBucket(), blob.getName());
      }
    }
    logger.info("Successfully deleted {} blobs; failed to delete {} blobs",
                deletedBlobs,
                deletableBlobs.size());
  }

  @Override
  public void run() {
    // step 1. check for finished jobs status; move blobs from claimed to deletable.
    checkJobs();
    // step 2. delete deletable blobs
    deleteBlobs();
    // step 3. get blobs to load into BQ.
    Map<TableId, List<Blob>> tablesToSourceURIs = getBlobsUpToLimit();
    // step 4. load blobs into BQ
    triggerBigQueryLoadJobs(tablesToSourceURIs);
  }
}