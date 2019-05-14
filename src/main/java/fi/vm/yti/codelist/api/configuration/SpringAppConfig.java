package fi.vm.yti.codelist.api.configuration;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.embedded.EmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.jetty.JettyEmbeddedServletContainerFactory;
import org.springframework.boot.web.servlet.ErrorPage;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.http.HttpStatus;
import org.springframework.web.filter.CommonsRequestLoggingFilter;

@Configuration
@PropertySource(value = "classpath", ignoreResourceNotFound = true)
public class SpringAppConfig {

    @Value("${yti_codelist_public_api_service_elastic_host}")
    protected String elasticsearchHost;

    @Value("${yti_codelist_public_api_service_elastic_port}")
    protected Integer elasticsearchPort;

    @Value("${yti_codelist_public_api_service_elastic_cluster}")
    protected String clusterName;

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
    public EmbeddedServletContainerFactory servletContainer() {
        final JettyEmbeddedServletContainerFactory factory = new JettyEmbeddedServletContainerFactory();
        factory.addErrorPages(new ErrorPage(HttpStatus.NOT_FOUND, "/notfound.html"));
        factory.setContextPath(contextPath);
        return factory;
    }

    @Bean
    @SuppressWarnings("resource")
    protected RestHighLevelClient elasticSearchRestHighLevelClient() {
        return new RestHighLevelClient(
            RestClient.builder(
                new HttpHost(elasticsearchHost, elasticsearchPort, "http")));
    }
}