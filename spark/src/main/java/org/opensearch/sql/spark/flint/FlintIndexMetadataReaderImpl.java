package org.opensearch.sql.spark.flint;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;

/** Implementation of {@link FlintIndexMetadataReader} */
@AllArgsConstructor
public class FlintIndexMetadataReaderImpl implements FlintIndexMetadataReader {

  private final Client client;

  @Override
  public FlintIndexMetadata getFlintIndexMetadata(IndexQueryDetails indexQueryDetails) {
    String indexName = indexQueryDetails.openSearchIndexName();
    GetMappingsResponse mappingsResponse =
        client.admin().indices().prepareGetMappings(indexName).get();
    try {
      MappingMetadata mappingMetadata = mappingsResponse.mappings().get(indexName);
      Map<String, Object> mappingSourceMap = mappingMetadata.getSourceAsMap();
      return FlintIndexMetadata.fromMetatdata((Map<String, Object>) mappingSourceMap.get("_meta"));
    } catch (NullPointerException npe) {
      throw new IllegalArgumentException("Provided index doesn't exist");
    }
  }

  // Returns Map of Flint index name -> metadata
  // TODO: determine whether to return Flint index name here or reconstruct from metadata
  @Override
  public Map<String, FlintIndexMetadata> getAllFlintIndexMetadata(IndexQueryDetails indexQueryDetails) {
    String catalogDbName =
        indexQueryDetails.getCatalogDb().replace(".", "_");
    String indexNamePattern = "flint_" + catalogDbName + "_*";
    GetMappingsResponse mappingsResponse =
        client.admin().indices().prepareGetMappings(indexNamePattern).get();
    try {
      return mappingsResponse.mappings().entrySet().stream()
          .collect(Collectors.toMap(
          entry -> entry.getKey(),
          entry -> {
            MappingMetadata mappingMetadata = entry.getValue();
            Map<String, Object> mappingSourceMap = mappingMetadata.getSourceAsMap();
            return FlintIndexMetadata.fromMetatdata((Map<String, Object>) mappingSourceMap.get("_meta"));
          }));
    } catch (NullPointerException npe) {
      throw new IllegalArgumentException("Provided index name pattern doesn't exist");
    }
  }
}
