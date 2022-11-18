package com.bcbsfl.es;

import java.io.File;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import javax.net.ssl.SSLContext;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ReconcileApp {
    private static final String ROW_LIMIT_PROPNAME = "row.limit";
    private static final String ROW_OFFSET_PROPNAME = "row.offset";
    protected static final String TASK_DOC_TYPE = "task";
    protected static final String WORKFLOW_DOC_TYPE = "workflow";
    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");
    protected static final SimpleDateFormat SIMPLE_MONTHLY_DATE_FORMAT = new SimpleDateFormat("yyyy.MM");
    protected static final SimpleDateFormat SIMPLE_YEARLY_DATE_FORMAT = new SimpleDateFormat("yyyy");
    protected static final SimpleDateFormat ES_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    protected RestHighLevelClient elasticSearchClient;
	protected FBElasticSearchDocTagger fbDocTagger = null;
	protected ObjectMapper objectMapper = new JsonMapperProvider().get();
    protected String indexName = null;
    protected String env = null;
    protected String outputDirectory = null;
    protected int rowLimit = 0;
    protected int rowOffset = 0;
    private static Long dsLock = new Long(0);
	private static HikariDataSource ds = null;
    
    protected String doctype = null;
    static {
        ES_TIMESTAMP_FORMAT.setTimeZone(GMT);
        SIMPLE_MONTHLY_DATE_FORMAT.setTimeZone(GMT);
        SIMPLE_YEARLY_DATE_FORMAT.setTimeZone(GMT);
    }
	public ReconcileApp(String doctype) {
		try {
            this.env = Utils.getProperty("env");
            this.outputDirectory = Utils.getProperty("output.directory");
	    	this.rowLimit = Utils.getIntProperty(ROW_LIMIT_PROPNAME);
	    	this.rowOffset = Utils.getIntProperty(ROW_OFFSET_PROPNAME);
            this.doctype = doctype;
	       	this.fbDocTagger = new FBElasticSearchDocTagger();
	       	this.elasticSearchClient = getRestHighLevelClient();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	protected void createDataSource() throws Exception {
		synchronized(dsLock) {
			if(ds == null) {
		    	HikariConfig config = new HikariConfig();
				config.setJdbcUrl(Utils.getProperty("db.url"));
				config.setUsername(Utils.getProperty("db.user"));
				config.setPassword(Utils.getProperty("db.password"));
				config.setMaximumPoolSize(5);
				config.setIdleTimeout(30000);
				config.setMinimumIdle(1);
		        config.setAutoCommit(false);
		        config.addDataSourceProperty("cachePrepStmts", "true");
		        config.addDataSourceProperty("prepStmtCacheSize", "250");
		        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");		
		        ds = new HikariDataSource(config);
			}
		}
	}

	protected void closeDataSource() throws Exception {
/* This doesn't get rid of the data source so connections will keep going if this is called and then another createDataSource()		
        ds.close();
*/        
	}

	protected RestHighLevelClient getRestHighLevelClient() throws Exception {

		CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(Utils.getProperty("es.user"), Utils.getProperty("es.password")));

		File file = new File("mykeystore");
		
		FileUtils.copyInputStreamToFile(getClass().getClassLoader().getResourceAsStream("my_keystore.jks"), file);
		
		final SSLContext sslContext = SSLContexts.custom()
				.loadTrustMaterial(file, "admin123".toCharArray(), new TrustSelfSignedStrategy()).build();

		RestClientBuilder clientBuilder = null;
		if(null == Utils.getProperty("es.url2") || "".equals(Utils.getProperty("es.url2"))) {
			clientBuilder = RestClient.builder(new HttpHost(Utils.getProperty("es.url1"), 9200, "https"));
		} else {
			clientBuilder = RestClient
				.builder(new HttpHost(Utils.getProperty("es.url1"), 9200, "https"),
					new HttpHost(Utils.getProperty("es.url2"), 9200, "https"));
		}
		clientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
			.setDefaultCredentialsProvider(credentialsProvider).setSSLContext(sslContext));

		RestHighLevelClient higLevelRestClient = new RestHighLevelClient(clientBuilder);

		return higLevelRestClient;
	}

    protected Connection getDatabaseConnection() {
    	Connection con = null;
		try {
			con = ds.getConnection();
		} catch(Exception e) {
			e.printStackTrace();
		}
		return con;
    }
    
    protected String getFileSuffix() {
		StringBuffer sb = new StringBuffer();
		String start = Utils.getProperty("db.startTimeframe");
		String end = Utils.getProperty("db.endTimeframe");
		if(StringUtils.isNotBlank(start)) {
			if(StringUtils.isNotBlank(end)) {
				sb.append("BETWEEN_");
				sb.append(start);
				sb.append("_AND_");
				sb.append(end);
			} else {
				sb.append("GT_");
				sb.append(start);
			}
		} else if(StringUtils.isNotBlank(end)) {
			sb.append("LT_");
			sb.append(end);
		}
		return sb.toString();
	}
}