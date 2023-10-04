package org.opensearch.sql.datasources.cloudwatchlog;

import static org.opensearch.sql.datasource.model.DataSourceType.CLOUDWATCHLOG;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.AuthenticationType;
import org.opensearch.sql.datasources.utils.DatasourceValidationUtils;
import org.opensearch.sql.storage.DataSourceFactory;

@RequiredArgsConstructor
public class CWLDataSourceFactory implements DataSourceFactory {
  private final Settings pluginSettings;

  // CWL configuration properties
  public static final String CWL_REGION = CLOUDWATCHLOG.getText() + ".region";
  public static final String CWL_AUTH_TYPE = CLOUDWATCHLOG.getText() + ".auth.type";
  public static final String CWL_AUTH_ROLE_ARN = CLOUDWATCHLOG.getText() + ".auth.role_arn";
  public static final String CWL_INDEX_STORE_OPENSEARCH_URI = CLOUDWATCHLOG.getText() + ".indexstore.opensearch.uri";
  public static final String CWL_INDEX_STORE_OPENSEARCH_AUTH = CLOUDWATCHLOG.getText() + ".indexstore.opensearch.auth";
  public static final String CWL_INDEX_STORE_OPENSEARCH_AUTH_USERNAME =
      CLOUDWATCHLOG.getText() + ".indexstore.opensearch.auth.username";
  public static final String CWL_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD =
      CLOUDWATCHLOG.getText() + ".indexstore.opensearch.auth.password";
  public static final String CWL_INDEX_STORE_OPENSEARCH_REGION = CLOUDWATCHLOG.getText() + ".indexstore.opensearch.region";

  @Override
  public DataSourceType getDataSourceType() {
    return CLOUDWATCHLOG;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    try {
      validateCWLDataSourceConfiguration(metadata.getProperties());
      return new DataSource(
          metadata.getName(),
          metadata.getConnector(), // TODO: or CLOUDWATCHLOG
          (dataSourceSchemaName, tableName) -> {
            throw new UnsupportedOperationException("CWL storage engine is not supported.");
          });
    } catch (URISyntaxException | UnknownHostException e) {
      throw new IllegalArgumentException("Invalid flint host in properties.");
    }
  }

  private void validateCWLDataSourceConfiguration(Map<String, String> dataSourceMetadataConfig)
      throws URISyntaxException, UnknownHostException {
    // TODO: duplicate code with Glue datasource
    DatasourceValidationUtils.validateLengthAndRequiredFields(
        dataSourceMetadataConfig,
        Set.of(
            CWL_REGION,
            CWL_AUTH_TYPE,
            CWL_AUTH_ROLE_ARN,
            CWL_INDEX_STORE_OPENSEARCH_URI,
            CWL_INDEX_STORE_OPENSEARCH_AUTH));

    AuthenticationType authenticationType =
        AuthenticationType.get(dataSourceMetadataConfig.get(CWL_INDEX_STORE_OPENSEARCH_AUTH));
    if (AuthenticationType.BASICAUTH.equals(authenticationType)) {
      DatasourceValidationUtils.validateLengthAndRequiredFields(
          dataSourceMetadataConfig,
          Set.of(
              CWL_INDEX_STORE_OPENSEARCH_AUTH_USERNAME,
              CWL_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD));
    } else if (AuthenticationType.AWSSIGV4AUTH.equals(authenticationType)) {
      DatasourceValidationUtils.validateLengthAndRequiredFields(
          dataSourceMetadataConfig, Set.of(CWL_INDEX_STORE_OPENSEARCH_REGION));
    }
    DatasourceValidationUtils.validateHost(
        dataSourceMetadataConfig.get(CWL_INDEX_STORE_OPENSEARCH_URI),
        pluginSettings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST));
  }
}
