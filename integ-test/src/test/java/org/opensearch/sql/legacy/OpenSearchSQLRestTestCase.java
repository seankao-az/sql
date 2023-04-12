/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.Timeout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.remote.RemoteInfoAction;
import org.opensearch.action.admin.cluster.remote.RemoteInfoRequest;
import org.opensearch.client.*;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.MockHttpTransport;
import org.opensearch.test.NodeConfigurationSource;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.RemoteClusterAware;
import org.opensearch.transport.RemoteConnectionInfo;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.nio.MockNioTransportPlugin;

import static java.util.Collections.unmodifiableList;
import static org.hamcrest.Matchers.*;
import static org.opensearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.opensearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;

/**
 * OpenSearch SQL integration test base class to support both security disabled and enabled OpenSearch cluster.
 */
public abstract class OpenSearchSQLRestTestCase extends OpenSearchRestTestCase {

  // copy from AbstractMultiClustersTestCase: start
  // with some modification
  public static final String LOCAL_CLUSTER = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

  private static volatile OpenSearchSQLRestTestCase.ClusterGroup clusterGroup;

  protected Collection<String> remoteClusterAlias() {
    return randomSubsetOf(Arrays.asList("cluster-a", "cluster-b"));
  }

  protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
    return Collections.emptyList();
  }

  protected final Client ccsClient() {
    return ccsClient(LOCAL_CLUSTER);
  }

  protected final Client ccsClient(String clusterAlias) {
    return cluster(clusterAlias).client();
  }

  protected final InternalTestCluster cluster(String clusterAlias) {
    return clusterGroup.getCluster(clusterAlias);
  }

  protected final Map<String, InternalTestCluster> clusters() {
    return Collections.unmodifiableMap(clusterGroup.clusters);
  }

  protected boolean reuseClusters() {
    return true;
  }

  private static Map<String, List<HttpHost>> ccsClusterHosts = new HashMap<>();

  public final void startClusters() throws Exception {
    if (clusterGroup != null && reuseClusters()) {
      return;
    }
    stopClusters();
    final Map<String, InternalTestCluster> clusters = new HashMap<>();
    final List<String> clusterAliases = new ArrayList<>(remoteClusterAlias());
    clusterAliases.add(LOCAL_CLUSTER);
    for (String clusterAlias : clusterAliases) {
      final String clusterName = clusterAlias.equals(LOCAL_CLUSTER) ? "main-cluster" : clusterAlias;
      final int numberOfNodes = randomIntBetween(1, 3);
      final List<Class<? extends Plugin>> mockPlugins = Arrays.asList(
          MockHttpTransport.TestPlugin.class,
          MockTransportService.TestPlugin.class,
          MockNioTransportPlugin.class
      );
      final Collection<Class<? extends Plugin>> nodePlugins = nodePlugins(clusterAlias);
      final Settings nodeSettings = Settings.EMPTY;
      final NodeConfigurationSource nodeConfigurationSource = nodeConfigurationSource(nodeSettings, nodePlugins);
      final InternalTestCluster cluster = new InternalTestCluster(
          randomLong(),
          createTempDir(),
          true,
          true,
          numberOfNodes,
          numberOfNodes,
          clusterName,
          nodeConfigurationSource,
          0,
          clusterName + "-",
          mockPlugins,
          Function.identity()
      );
      cluster.beforeTest(random());
      clusters.put(clusterAlias, cluster);
    }
    clusterGroup = new OpenSearchSQLRestTestCase.ClusterGroup(clusters);
    configureAndConnectsToRemoteClusters();

    // HttpHosts for each cluster
    for (String clusterAlias : clusterGroup.clusterAliases()) {
      final InternalTestCluster cluster = clusterGroup.getCluster(clusterAlias);
      final List<String> allNodes = Arrays.asList(cluster.getNodeNames());
      final List<String> stringUrls = allNodes
          .stream()
          .map(node -> cluster(clusterAlias).getInstance(TransportService.class, node)
              .boundAddress().publishAddress().toString())
          .collect(Collectors.toList());
      List<HttpHost> hosts = new ArrayList<>(stringUrls.size());
      for (String stringUrl : stringUrls) {
        int portSeparator = stringUrl.lastIndexOf(':');
        if (portSeparator < 0) {
          throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
        }
        String host = stringUrl.substring(0, portSeparator);
        int port = Integer.valueOf(stringUrl.substring(portSeparator + 1));
        hosts.add(buildHttpHost(host, port));
      }
      ccsClusterHosts.put(clusterAlias, unmodifiableList(hosts));
    }

    assert ccsClusterHosts.containsKey("");
  }

  private static Map<String, RestClient> ccsRestClient = new HashMap<>();
  private static Map<String, RestClient> ccsRestAdminClient = new HashMap<>();
  private static Map<String, TreeSet<Version>> ccsNodeVersions = new HashMap<>();

  // modified from initClient in OpenSearchRestTestCase
  public void initRestClient(String clusterAlias) throws IOException {
    if (!ccsRestClient.containsKey(clusterAlias)) {
      assert !ccsRestAdminClient.containsKey(clusterAlias);
      assert !ccsNodeVersions.containsKey(clusterAlias);

      final List<HttpHost> hosts = ccsClusterHosts.get(clusterAlias);
      logger.info("initializing REST clients against {}", hosts);

      final RestClient restClient = buildClient(restClientSettings(), hosts.toArray(new HttpHost[0]));
      final RestClient restAdminClient = buildClient(restAdminSettings(), hosts.toArray(new HttpHost[0]));
      ccsRestClient.put(clusterAlias, restClient);
      ccsRestAdminClient.put(clusterAlias, restAdminClient);

      ccsNodeVersions.put(clusterAlias, new TreeSet<>());
      Map<?, ?> response = entityAsMap(restAdminClient.performRequest(new Request("GET", "_nodes/plugins")));
      Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");
      for (Map.Entry<?, ?> node : nodes.entrySet()) {
        Map<?, ?> nodeInfo = (Map<?, ?>) node.getValue();
        ccsNodeVersions.get(clusterAlias).add(Version.fromString(nodeInfo.get("version").toString()));
      }
    }
    assert ccsRestClient.get(clusterAlias) != null;
    assert ccsRestAdminClient.get(clusterAlias) != null;
    assert ccsNodeVersions.get(clusterAlias) != null;
  }

  @Before
  public final void startup() throws Exception {
    startClusters();
    for (String clusterAlias : clusterGroup.clusterAliases()) {
      initRestClient(clusterAlias);
    }
  }

  @After
  public void assertAfterTest() throws Exception {
    for (InternalTestCluster cluster : clusters().values()) {
      cluster.wipe(Collections.emptySet());
      cluster.assertAfterTest();
    }
  }

  @AfterClass
  public static void stopClusters() throws IOException {
    IOUtils.close(clusterGroup);
    clusterGroup = null;
  }

  protected void disconnectFromRemoteClusters() throws Exception {
    Settings.Builder settings = Settings.builder();
    final Set<String> clusterAliases = clusterGroup.clusterAliases();
    for (String clusterAlias : clusterAliases) {
      if (clusterAlias.equals(LOCAL_CLUSTER) == false) {
        settings.putNull("cluster.remote." + clusterAlias + ".seeds");
      }
    }
    ccsClient().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
    assertBusy(() -> {
      for (TransportService transportService : cluster(LOCAL_CLUSTER).getInstances(TransportService.class)) {
        assertThat(transportService.getRemoteClusterService().getRegisteredRemoteClusterNames(), empty());
      }
    });
  }

  protected void configureAndConnectsToRemoteClusters() throws Exception {
    Map<String, List<String>> seedNodes = new HashMap<>();
    for (String clusterAlias : clusterGroup.clusterAliases()) {
      if (clusterAlias.equals(LOCAL_CLUSTER) == false) {
        final InternalTestCluster cluster = clusterGroup.getCluster(clusterAlias);
        final String[] allNodes = cluster.getNodeNames();
        final List<String> selectedNodes = randomSubsetOf(randomIntBetween(1, Math.min(3, allNodes.length)), allNodes);
        seedNodes.put(clusterAlias, selectedNodes);
      }
    }
    if (seedNodes.isEmpty()) {
      return;
    }
    Settings.Builder settings = Settings.builder();
    for (Map.Entry<String, List<String>> entry : seedNodes.entrySet()) {
      final String clusterAlias = entry.getKey();
      final String seeds = entry.getValue()
          .stream()
          .map(node -> cluster(clusterAlias).getInstance(TransportService.class, node)
              .boundAddress().publishAddress().toString())
          .collect(Collectors.joining(","));
      settings.put("cluster.remote." + clusterAlias + ".seeds", seeds);
    }
    ccsClient().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
    assertBusy(() -> {
      List<RemoteConnectionInfo> remoteConnectionInfos = ccsClient().execute(RemoteInfoAction.INSTANCE, new RemoteInfoRequest())
          .actionGet()
          .getInfos()
          .stream()
          .filter(RemoteConnectionInfo::isConnected)
          .collect(Collectors.toList());
      final long totalConnections = seedNodes.values().stream().map(List::size).count();
      assertThat(remoteConnectionInfos, hasSize(Math.toIntExact(totalConnections)));
    });
  }

  static class ClusterGroup implements Closeable {
    private final Map<String, InternalTestCluster> clusters;

    ClusterGroup(Map<String, InternalTestCluster> clusters) {
      this.clusters = Collections.unmodifiableMap(clusters);
    }

    InternalTestCluster getCluster(String clusterAlias) {
      assertThat(clusters, hasKey(clusterAlias));
      return clusters.get(clusterAlias);
    }

    Set<String> clusterAliases() {
      return clusters.keySet();
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(clusters.values());
    }
  }

  static NodeConfigurationSource nodeConfigurationSource(Settings nodeSettings, Collection<Class<? extends Plugin>> nodePlugins) {
    final Settings.Builder builder = Settings.builder();
    builder.putList(DISCOVERY_SEED_HOSTS_SETTING.getKey()); // empty list disables a port scan for other nodes
    builder.putList(DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "file");
    builder.put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType());
    builder.put(nodeSettings);

    return new NodeConfigurationSource() {
      @Override
      public Settings nodeSettings(int nodeOrdinal) {
        return builder.build();
      }

      @Override
      public Path nodeConfigPath(int nodeOrdinal) {
        return null;
      }

      @Override
      public Collection<Class<? extends Plugin>> nodePlugins() {
        return nodePlugins;
      }
    };
  }
  // copy from AbstractMultiClustersTestCase: end
  private static final Logger LOG = LogManager.getLogger();

  protected boolean isHttps() {
    boolean isHttps = Optional.ofNullable(System.getProperty("https"))
        .map("true"::equalsIgnoreCase).orElse(false);
    if (isHttps) {
      //currently only external cluster is supported for security enabled testing
      if (!Optional.ofNullable(System.getProperty("tests.rest.cluster")).isPresent()) {
        throw new RuntimeException(
            "external cluster url should be provided for security enabled testing");
      }
    }

    return isHttps;
  }

  protected String getProtocol() {
    return isHttps() ? "https" : "http";
  }

  protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
    RestClientBuilder builder = RestClient.builder(hosts);
    if (isHttps()) {
      configureHttpsClient(builder, settings, hosts[0]);
    } else {
      configureClient(builder, settings);
    }

    builder.setStrictDeprecationMode(false);
    return builder.build();
  }

  protected static void wipeAllOpenSearchIndices() throws IOException {
    // include all the indices, included hidden indices.
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-indices.html#cat-indices-api-query-params
    try {
      Response response = client().performRequest(new Request("GET", "/_cat/indices?format=json&expand_wildcards=all"));
      JSONArray jsonArray = new JSONArray(EntityUtils.toString(response.getEntity(), "UTF-8"));
      for (Object object : jsonArray) {
        JSONObject jsonObject = (JSONObject) object;
        String indexName = jsonObject.getString("index");
        try {
          // System index, mostly named .opensearch-xxx or .opendistro-xxx, are not allowed to delete
          if (!indexName.startsWith(".opensearch") && !indexName.startsWith(".opendistro")) {
            client().performRequest(new Request("DELETE", "/" + indexName));
          }
        } catch (Exception e) {
          // TODO: Ignore index delete error for now. Remove this if strict check on system index added above.
          LOG.warn("Failed to delete index: " + indexName, e);
        }
      }
    } catch (ParseException e) {
      throw new IOException(e);
    }
  }

  protected static void configureHttpsClient(RestClientBuilder builder, Settings settings,
                                             HttpHost httpHost)
      throws IOException {
    Map<String, String> headers = ThreadContext.buildDefaultHeaders(settings);
    Header[] defaultHeaders = new Header[headers.size()];
    int i = 0;
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
    }
    builder.setDefaultHeaders(defaultHeaders);
    builder.setHttpClientConfigCallback(httpClientBuilder -> {
      String userName = Optional.ofNullable(System.getProperty("user"))
          .orElseThrow(() -> new RuntimeException("user name is missing"));
      String password = Optional.ofNullable(System.getProperty("password"))
          .orElseThrow(() -> new RuntimeException("password is missing"));
      BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider
          .setCredentials(new AuthScope(httpHost), new UsernamePasswordCredentials(userName,
              password.toCharArray()));
      try {
        final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
            .setSslContext(SSLContextBuilder.create()
                .loadTrustMaterial(null, (chains, authType) -> true)
                .build())
            .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
            .build();

        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
            .setConnectionManager(PoolingAsyncClientConnectionManagerBuilder.create()
                .setTlsStrategy(tlsStrategy)
                .build());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
    final TimeValue socketTimeout =
        TimeValue.parseTimeValue(socketTimeoutString == null ? "60s" : socketTimeoutString,
            CLIENT_SOCKET_TIMEOUT);
    builder.setRequestConfigCallback(
        conf -> conf.setResponseTimeout(Timeout.ofMilliseconds(Math.toIntExact(socketTimeout.getMillis()))));
    if (settings.hasValue(CLIENT_PATH_PREFIX)) {
      builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
    }
  }
}
