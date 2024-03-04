/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;
import static org.opensearch.sql.spark.execution.statestore.StateStore.createIndexManagementResult;

import com.amazonaws.services.emrserverless.model.JobRunState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.client.Client;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.dispatcher.model.*;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexMetadataReader;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

/** Handle Index Management query. includes SHOW */
@RequiredArgsConstructor
public class IndexManagementHandler extends AsyncQueryHandler {
  private static final Logger LOG = LogManager.getLogger();

  public static final String SHOW_FLINT_INDEX_JOB_ID = "showFlintIndexJobId";
  // TODO: remove unused members
  // TODO: merge with IndexDMLHandler and distinguish DROP INDEX and SHOW FLINT INDEX there

  private final JobExecutionResponseReader jobExecutionResponseReader;

  private final FlintIndexMetadataReader flintIndexMetadataReader;

  private final Client client;

  private final StateStore stateStore;

  public static boolean isIndexManagementQuery(String jobId) {
    return SHOW_FLINT_INDEX_JOB_ID.equalsIgnoreCase(jobId);
  }

  @Override
  public DispatchQueryResponse submit(
      DispatchQueryRequest dispatchQueryRequest, DispatchQueryContext context) {
    DataSourceMetadata dataSourceMetadata = context.getDataSourceMetadata();
    IndexQueryDetails indexDetails = context.getIndexQueryDetails();

    String status = JobRunState.FAILED.toString();
    String error = "";
    List<String> result = new ArrayList<>();
    long startTime = 0L;
    try {
      Map<String, FlintIndexMetadata> allIndexMetadata = flintIndexMetadataReader.getAllFlintIndexMetadata(indexDetails);

      // get state and lastupdatetime from .query_execution_request_{datasource}

      result = allIndexMetadata.entrySet().stream()
          .map(entry -> parseResultEntryFromMetadata(entry.getKey(), entry.getValue()))
          .collect(Collectors.toList());

      status = JobRunState.SUCCESS.toString();
    } catch (Exception e) {
      error = e.getMessage();
      LOG.error(e);
    }

    AsyncQueryId asyncQueryId = AsyncQueryId.newAsyncQueryId(dataSourceMetadata.getName());
    IndexManagementResult indexManagementResult =
        new IndexManagementResult(
            asyncQueryId.getId(),
            status,
            error,
            dispatchQueryRequest.getDatasource(),
            System.currentTimeMillis() - startTime,
            System.currentTimeMillis(),
            result);
    String resultIndex = dataSourceMetadata.getResultIndex();
    createIndexManagementResult(stateStore, resultIndex).apply(indexManagementResult);

    return new DispatchQueryResponse(asyncQueryId, SHOW_FLINT_INDEX_JOB_ID, resultIndex, null);
  }

  private String parseResultEntryFromMetadata(String flintIndexName, FlintIndexMetadata metadata) {
    String databaseName = StringUtils.EMPTY;
    String tableName = StringUtils.EMPTY;
    String indexName = StringUtils.EMPTY;
    String[] parts;

    // TODO
    String status = "NOT IMPLEMENTED";

    String kind = metadata.getKind();
    // TODO: refactor
    switch (kind) {
      // TODO: parts exception handling
      case "covering":
        parts = metadata.getSource().split("\\.");
        databaseName = parts[1];
        tableName = String.join(".", Arrays.copyOfRange(parts, 2, parts.length));
        indexName = metadata.getName();
        break;
      case "skipping":
        parts = metadata.getSource().split("\\.");
        databaseName = parts[1];
        tableName = String.join(".", Arrays.copyOfRange(parts, 2, parts.length));
        break;
      case "mv":
        parts = metadata.getName().split("\\.");
        databaseName = parts[1];
        indexName = String.join(".", Arrays.copyOfRange(parts, 2, parts.length));
        break;
    }
    return new JSONObject()
        .put("flint_index_name", flintIndexName)
        .put("kind", kind)
        .put("database", databaseName)
        .put("table", tableName)
        .put("index_name", indexName)
        .put("auto_refresh", metadata.isAutoRefresh())
        .put("status", status)
        .toString();
  }

  @Override
  protected JSONObject getResponseFromResultIndex(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    String queryId = asyncQueryJobMetadata.getQueryId().getId();
    return jobExecutionResponseReader.getResultWithQueryId(
        queryId, asyncQueryJobMetadata.getResultIndex());
  }

  @Override
  protected JSONObject getResponseFromExecutor(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    // Consider statement still running if result doc created in submit() is not available yet
    JSONObject result = new JSONObject();
    result.put(STATUS_FIELD, StatementState.RUNNING.getState());
    result.put(ERROR_FIELD, "");
    return result;
  }

  @Override
  public String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    throw new IllegalArgumentException("can't cancel index management query");
  }
}
