/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

/** Enum for Index Action in the given query.* */
public enum IndexQueryActionType {
  CREATE,
  REFRESH,
  DESCRIBE,
  SHOW,
  DROP,
  SHOW_IN_CATALOGDB // TODO: or SHOW_FLINT? It's different from SHOW for one kind of index
}
