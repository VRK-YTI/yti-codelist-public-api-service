package fi.vm.yti.codelist.api.configuration;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

import fi.vm.yti.codelist.api.resource.CodeRegistryResource;
import fi.vm.yti.codelist.api.resource.CodeSchemeResource;
import fi.vm.yti.codelist.api.resource.ExternalReferenceResource;
import fi.vm.yti.codelist.api.resource.PingResource;
import fi.vm.yti.codelist.api.resource.PropertyTypeResource;
import fi.vm.yti.codelist.api.resource.SwaggerResource;
import fi.vm.yti.codelist.api.resource.VersionResource;
import fi.vm.yti.codelist.common.constants.ApiConstants;
import io.swagger.annotations.Api;
import io.swagger.annotations.Contact;
import io.swagger.annotations.Info;
import io.swagger.annotations.License;
import io.swagger.annotations.SwaggerDefinition;

@Component
@SwaggerDefinition(
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
    host = "localhost:9601",
    basePath = ApiConstants.API_CONTEXT_PATH_RESTAPI + ApiConstants.API_BASE_PATH,
    consumes = {MediaType.APPLICATION_JSON},
    produces = {MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN, "application/csv", "application/xls", "application/xlsx"},
    schemes = {SwaggerDefinition.Scheme.HTTPS}
)
@Api(value = ApiConstants.API_BASE_PATH, description = "Code List Service - Public API Service")
@ApplicationPath(ApiConstants.API_BASE_PATH)
public class JerseyConfig extends ResourceConfig {

    public JerseyConfig() {
        final JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
        provider.setMapper(new CustomObjectMapper());

        // CORS filtering.
        register(CorsFilter.class);

        // Cache control headers to no cache.
        register(CacheFilter.class);

        // Health.
        register(PingResource.class);

        // Generic resources.
        register(VersionResource.class);
        register(SwaggerResource.class);

        // API: Generic Register resources.
        register(CodeRegistryResource.class);
        register(CodeSchemeResource.class);
        register(PropertyTypeResource.class);
        register(ExternalReferenceResource.class);
    }
}
