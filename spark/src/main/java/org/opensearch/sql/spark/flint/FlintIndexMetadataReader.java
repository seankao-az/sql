package org.opensearch.sql.spark.flint;

import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;

import java.util.Map;

/** Interface for FlintIndexMetadataReader */
public interface FlintIndexMetadataReader {

  /**
   * Given Index details, get the streaming job Id.
   *
   * @param indexQueryDetails indexDetails.
   * @return FlintIndexMetadata.
   */
  FlintIndexMetadata getFlintIndexMetadata(IndexQueryDetails indexQueryDetails);

  /**
   *
   * @param indexQueryDetails
   * @return
   */
  Map<String, FlintIndexMetadata> getAllFlintIndexMetadata(IndexQueryDetails indexQueryDetails);
}
