/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import org.opensearch.sql.calcite.remote.fallback.CalciteConvertTZFunctionIT;

public class NonFallbackCalciteConvertTZFunctionIT extends CalciteConvertTZFunctionIT {
  @Override
  public void init() throws Exception {
    super.init();
    disallowCalciteFallback();
  }
}
