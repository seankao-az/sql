/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

import java.util.List;

import com.google.common.collect.ImmutableList;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.sql.spark.execution.statestore.StateModel;

import java.io.IOException;

import static org.opensearch.sql.spark.execution.session.SessionModel.DATASOURCE_NAME;

/** Plugin create Index DML result. */
@Data
@EqualsAndHashCode(callSuper = false)
public class IndexManagementResult extends StateModel {
  private static final String QUERY_ID = "queryId";
  private static final String QUERY_RUNTIME = "queryRunTime";
  private static final String UPDATE_TIME = "updateTime";
  private static final String DOC_ID_PREFIX = "index";

  private final String queryId;
  private final String status;
  private final String error;
  private final String datasourceName;
  private final Long queryRunTime;
  private final Long updateTime;
  private final List<String> result;
  private final List<String> schema = ImmutableList.of(
      "{'column_name': 'flint_index_name', 'data_type': 'string'}",
      "{'column_name': 'kind', 'data_type': 'string'}",
      "{'column_name': 'database', 'data_type': 'string'}",
      "{'column_name': 'table', 'data_type': 'string'}",
      "{'column_name': 'index_name', 'data_type': 'string'}",
      "{'column_name': 'auto_refresh', 'data_type': 'boolean'}",
      "{'column_name': 'status', 'data_type': 'string'}");
  // TODO: status (index state)
  // TODO: last update time


  public static IndexManagementResult copy(IndexManagementResult copy, long seqNo, long primaryTerm) {
    return new IndexManagementResult(
        copy.queryId,
        copy.status,
        copy.error,
        copy.datasourceName,
        copy.queryRunTime,
        copy.updateTime,
        copy.result);
  }

  @Override
  public String getId() {
    return DOC_ID_PREFIX + queryId;
  }

  @Override
  public long getSeqNo() {
    return SequenceNumbers.UNASSIGNED_SEQ_NO;
  }

  @Override
  public long getPrimaryTerm() {
    return SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
  }

  // TODO: how to construct the schema and result fields?
  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    builder
        .startObject()
        .field(QUERY_ID, queryId)
        .field("status", status)
        .field("error", error)
        .field(DATASOURCE_NAME, datasourceName)
        .field(QUERY_RUNTIME, queryRunTime)
        .field(UPDATE_TIME, updateTime)
        .field("result", result)
        .field("schema", schema)
        .endObject();
    return builder;
  }
}
