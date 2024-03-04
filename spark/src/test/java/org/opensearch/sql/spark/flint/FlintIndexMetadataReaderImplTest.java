package org.opensearch.sql.spark.flint;

import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.dispatcher.model.FullyQualifiedTableName;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryActionType;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;

@ExtendWith(MockitoExtension.class)
public class FlintIndexMetadataReaderImplTest {
  @Mock(answer = RETURNS_DEEP_STUBS)
  private Client client;

  @SneakyThrows
  @Test
  void testGetJobIdFromFlintSkippingIndexMetadata() {
    URL url =
        Resources.getResource(
            "flint-index-mappings/flint_mys3_default_http_logs_skipping_index.json");
    String mappings = Resources.toString(url, Charsets.UTF_8);
    String indexName = "flint_mys3_default_http_logs_skipping_index";
    mockNodeClientIndicesMappings(indexName, mappings);
    FlintIndexMetadataReader flintIndexMetadataReader = new FlintIndexMetadataReaderImpl(client);
    FlintIndexMetadata indexMetadata =
        flintIndexMetadataReader.getFlintIndexMetadata(
            IndexQueryDetails.builder()
                .fullyQualifiedTableName(new FullyQualifiedTableName("mys3.default.http_logs"))
                .autoRefresh(false)
                .indexQueryActionType(IndexQueryActionType.DROP)
                .indexType(FlintIndexType.SKIPPING)
                .build());
    Assertions.assertEquals("00fdmvv9hp8u0o0q", indexMetadata.getJobId());
  }

  @SneakyThrows
  @Test
  void testGetJobIdFromFlintCoveringIndexMetadata() {
    URL url =
        Resources.getResource("flint-index-mappings/flint_mys3_default_http_logs_cv1_index.json");
    String mappings = Resources.toString(url, Charsets.UTF_8);
    String indexName = "flint_mys3_default_http_logs_cv1_index";
    mockNodeClientIndicesMappings(indexName, mappings);
    FlintIndexMetadataReader flintIndexMetadataReader = new FlintIndexMetadataReaderImpl(client);
    FlintIndexMetadata indexMetadata =
        flintIndexMetadataReader.getFlintIndexMetadata(
            IndexQueryDetails.builder()
                .indexName("cv1")
                .fullyQualifiedTableName(new FullyQualifiedTableName("mys3.default.http_logs"))
                .autoRefresh(false)
                .indexQueryActionType(IndexQueryActionType.DROP)
                .indexType(FlintIndexType.COVERING)
                .build());
    Assertions.assertEquals("00fdmvv9hp8u0o0q", indexMetadata.getJobId());
  }

  @SneakyThrows
  @Test
  void testGetJobIdFromGetAllFlintIndexMetadata() {
    URL url1 =
        Resources.getResource("flint-index-mappings/flint_mys3_default_http_logs_skipping_index.json");
    String mappings1 = Resources.toString(url1, Charsets.UTF_8);
    String indexName1 = "flint_mys3_default_http_logs_skipping_index";
    URL url2 =
        Resources.getResource("flint-index-mappings/flint_mys3_default_http_logs_cv1_index.json");
    String mappings2 = Resources.toString(url2, Charsets.UTF_8);
    String indexName2 = "flint_mys3_default_http_logs_cv1_index";
    Map<String, String> allMappings = Map.of(indexName1, mappings1, indexName2, mappings2);
    mockNodeClientMultipleIndicesMappings(allMappings);

    FlintIndexMetadataReader flintIndexMetadataReader = new FlintIndexMetadataReaderImpl(client);
    Map<String, FlintIndexMetadata> indexMetadatas =
        flintIndexMetadataReader.getAllFlintIndexMetadata(
            IndexQueryDetails.builder()
                .catalogDb("mys3")
                .build());
    Assertions.assertEquals("00fdmvv9hp8u0o0q", indexMetadatas.get(indexName1).getJobId());
    Assertions.assertEquals("00fdmvv9hp8u0o0q", indexMetadatas.get(indexName2).getJobId());
  }

  @SneakyThrows
  @Test
  void testGetJobIDWithNPEException() {
    URL url = Resources.getResource("flint-index-mappings/npe_mapping.json");
    String mappings = Resources.toString(url, Charsets.UTF_8);
    String indexName = "flint_mys3_default_http_logs_cv1_index";
    mockNodeClientIndicesMappings(indexName, mappings);
    FlintIndexMetadataReader flintIndexMetadataReader = new FlintIndexMetadataReaderImpl(client);
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                flintIndexMetadataReader.getFlintIndexMetadata(
                    IndexQueryDetails.builder()
                        .indexName("cv1")
                        .fullyQualifiedTableName(
                            new FullyQualifiedTableName("mys3.default.http_logs"))
                        .autoRefresh(false)
                        .indexQueryActionType(IndexQueryActionType.DROP)
                        .indexType(FlintIndexType.COVERING)
                        .build()));
    Assertions.assertEquals("Provided Index doesn't exist", illegalArgumentException.getMessage());
  }

  public void mockNodeClientIndicesMappings(String indexName, String mappings) {
    GetMappingsResponse mockResponse = mock(GetMappingsResponse.class);
    when(client.admin().indices().prepareGetMappings(any()).get()).thenReturn(mockResponse);
    Map<String, MappingMetadata> metadata;
    metadata = Map.of(indexName, parseMappingMetadata(mappings));
    when(mockResponse.mappings()).thenReturn(metadata);
  }

  public void mockNodeClientMultipleIndicesMappings(Map<String, String> allMappings) {
    GetMappingsResponse mockResponse = mock(GetMappingsResponse.class);
    when(client.admin().indices().prepareGetMappings(any()).get()).thenReturn(mockResponse);
    Map<String, MappingMetadata> metadata;
    metadata = allMappings.entrySet().stream()
        .collect(Collectors.toMap(
            entry -> entry.getKey(),
            entry -> parseMappingMetadata(entry.getValue())));
    when(mockResponse.mappings()).thenReturn(metadata);
  }

  @SneakyThrows
  private MappingMetadata parseMappingMetadata(String mappings) {
    return IndexMetadata.fromXContent(createParser(mappings)).mapping();
  }

  private XContentParser createParser(String mappings) throws IOException {
    return XContentType.JSON
        .xContent()
        .createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappings);
  }
}
