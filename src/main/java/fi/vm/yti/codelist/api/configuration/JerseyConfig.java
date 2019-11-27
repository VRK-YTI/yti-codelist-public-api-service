package fi.vm.yti.codelist.api.configuration;

import javax.ws.rs.ApplicationPath;

import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.api.exception.exceptionmapping.YtiCodeListExceptionMapper;
import fi.vm.yti.codelist.api.filter.CacheFilter;
import fi.vm.yti.codelist.api.filter.CharsetResponseFilter;
import fi.vm.yti.codelist.api.filter.RequestLoggingFilter;
import fi.vm.yti.codelist.api.filter.RobotsFilter;
import fi.vm.yti.codelist.api.resource.CodeRegistryResource;
import fi.vm.yti.codelist.api.resource.CodeSchemeResource;
import fi.vm.yti.codelist.api.resource.ExtensionResource;
import fi.vm.yti.codelist.api.resource.ExternalReferenceResource;
import fi.vm.yti.codelist.api.resource.IntegrationResource;
import fi.vm.yti.codelist.api.resource.MemberResource;
import fi.vm.yti.codelist.api.resource.PingResource;
import fi.vm.yti.codelist.api.resource.PropertyTypeResource;
import fi.vm.yti.codelist.api.resource.UriResolverResource;
import fi.vm.yti.codelist.api.resource.ValueTypeResource;
import fi.vm.yti.codelist.api.resource.VersionResource;
import fi.vm.yti.codelist.common.constants.ApiConstants;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.servers.Server;

@Component
@OpenAPIDefinition(
    info = @Info(
        description = "YTI Codelist Service - Public API Service - Spring Boot microservice.",
        version = ApiConstants.API_VERSION,
        title = "YTI Codelist Service - Public API Service",
        termsOfService = "https://opensource.org/licenses/EUPL-1.1",
        contact = @Contact(
            name = "YTI Codelist Service by the Population Register Center of Finland",
            url = "https://yhteentoimiva.suomi.fi/",
            email = "yhteentoimivuus@vrk.fi"
        ),
        license = @License(
            name = "EUPL-1.2",
            url = "https://opensource.org/licenses/EUPL-1.1"
        )
    ),
    servers = {
        @Server(
            description = "Codelist Public API Service",
            url = "/codelist-api")
    }
)
@ApplicationPath(ApiConstants.API_BASE_PATH)
public class JerseyConfig extends ResourceConfig {

    public JerseyConfig() {
        final JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
        provider.setMapper(new CustomObjectMapper());

        // ExceptionMappers
        register(YtiCodeListExceptionMapper.class);

        // Charset filter
        register(CharsetResponseFilter.class);

        // Cache control headers to no cache.
        register(CacheFilter.class);

        // Logging
        register(RequestLoggingFilter.class);

        // X-Robots-Tag filter
        register(RobotsFilter.class);

        // Health
        register(PingResource.class);

        // Generic resources
        register(VersionResource.class);

        // OpenAPI
        register(OpenApiResource.class);

        // API: Generic Register resources
        register(CodeRegistryResource.class);
        register(CodeSchemeResource.class);
        register(PropertyTypeResource.class);
        register(ExternalReferenceResource.class);
        register(ExtensionResource.class);
        register(MemberResource.class);
        register(ValueTypeResource.class);

        // API: Integration API
        register(IntegrationResource.class);

        // API: URI Resolver
        register(UriResolverResource.class);
    }
}
