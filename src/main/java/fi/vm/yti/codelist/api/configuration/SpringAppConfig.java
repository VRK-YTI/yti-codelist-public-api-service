package fi.vm.yti.codelist.api.configuration;

import org.apache.catalina.connector.Connector;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.ErrorPage;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.http.HttpStatus;
import org.springframework.web.filter.CommonsRequestLoggingFilter;

@Configuration
@PropertySource(value = "classpath", ignoreResourceNotFound = true)
public class SpringAppConfig {

    private static final int ES_CONNECTION_TIMEOUT = 300000;
    private static final int ES_RETRY_TIMEOUT = 60000;

    @Value("${yti_codelist_public_api_service_elastic_host}")
    protected String elasticsearchHost;

    @Value("${yti_codelist_public_api_service_elastic_port}")
    protected Integer elasticsearchPort;

    @Value(value = "${application.contextPath}")
    private String contextPath;

    @Bean
    public CommonsRequestLoggingFilter requestLoggingFilter() {
        CommonsRequestLoggingFilter loggingFilter = new CommonsRequestLoggingFilter();
        loggingFilter.setIncludeClientInfo(true);
        loggingFilter.setIncludeQueryString(true);
        loggingFilter.setIncludePayload(true);
        return loggingFilter;
    }

    @Bean
    public TomcatServletWebServerFactory servletContainer(@Value("${tomcat.ajp.port:}") final Integer ajpPort) {
        final TomcatServletWebServerFactory tomcat = new TomcatServletWebServerFactory();
        tomcat.setContextPath(contextPath);
        tomcat.addErrorPages(new ErrorPage(HttpStatus.NOT_FOUND, "/notfound.html"));
        if (ajpPort != null) {
            final Connector ajpConnector = new Connector("AJP/1.3");
            ajpConnector.setPort(ajpPort);
            ajpConnector.setSecure(false);
            ajpConnector.setAllowTrace(false);
            ajpConnector.setScheme("http");
            tomcat.addAdditionalTomcatConnectors(ajpConnector);
        }
        return tomcat;
    }

    @Bean
    @SuppressWarnings("resource")
    protected RestHighLevelClient elasticSearchRestHighLevelClient() {
        final RestClientBuilder builder = RestClient.builder(
            new HttpHost(elasticsearchHost, elasticsearchPort, "http"))
            .setRequestConfigCallback(
                requestConfigBuilder -> requestConfigBuilder
                    .setConnectTimeout(ES_CONNECTION_TIMEOUT)
                    .setSocketTimeout(ES_CONNECTION_TIMEOUT));
        return new RestHighLevelClient(builder);
    }
}
