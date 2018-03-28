package fi.vm.yti.codelist.api.resource;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fi.vm.yti.codelist.api.api.ApiUtils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import static fi.vm.yti.codelist.common.constants.ApiConstants.API_PATH_VERSION_V1;
import static fi.vm.yti.codelist.common.constants.ApiConstants.METHOD_GET;

@Component
@Path("/v1/uris")
@Api(value = "uris")
@Produces({MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv"})
public class UriResolverResource extends AbstractBaseResource {

    private static final Logger LOG = LoggerFactory.getLogger(UriResolverResource.class);
    private static final String SUOMI_URI_HOST = "uri.suomi.fi";
    private static final String API_PATH_URIRESOURCE = "/uris";
    private static final String API_PATH_RESOLVE = "/resolve";
    private static final String API_PATH_CODELIST = "/codelist";

    private final ApiUtils apiUtils;

    @Inject
    public UriResolverResource(final ApiUtils apiUtils) {
        this.apiUtils = apiUtils;
    }

    @GET
    @Path("resolve")
    @ApiOperation(value = "Resolve URI resource.", response = String.class)
    @ApiResponse(code = 200, message = "Resolves the API url for the given codelist resource URI.")
    @Produces({MediaType.APPLICATION_JSON + ";charset=UTF-8", MediaType.TEXT_PLAIN})
    public Response resolveUri(@ApiParam(value = "Resource URI.", required = true) @QueryParam("uri") final String uri) {
        logApiRequest(LOG, METHOD_GET, API_PATH_VERSION_V1, API_PATH_URIRESOURCE + API_PATH_RESOLVE);
        final URI resolveUri = parseUriFromString(uri);
        ensureSuomiFiUriHost(resolveUri.getHost());
        final String uriPath = resolveUri.getPath();
        final ObjectMapper objectMapper = new ObjectMapper();
        final ObjectNode json = objectMapper.createObjectNode();
        json.put("uri", uri);
        json.put("url", resolveResourceUrl(uriPath));
        return Response.ok().entity(json).build();
    }

    @GET
    @Path("redirect")
    @ApiOperation(value = "Redirect URI resource.", response = String.class)
    @ApiResponse(code = 200, message = "Does a redirect from codelist resource URI to codelist API.")
    public Response redirectUri(@ApiParam(value = "Resource URI.", required = true) @QueryParam("uri") final String uri) {
        logApiRequest(LOG, METHOD_GET, API_PATH_VERSION_V1, API_PATH_URIRESOURCE + API_PATH_RESOLVE);
        final URI resolveUri = parseUriFromString(uri);
        ensureSuomiFiUriHost(resolveUri.getHost());
        final String uriPath = resolveUri.getPath();
        final URI redirectUrl = URI.create(resolveResourceUrl(uriPath));
        return Response.temporaryRedirect(redirectUrl).build();
    }

    private void ensureSuomiFiUriHost(final String host) {
        if (!SUOMI_URI_HOST.equalsIgnoreCase(host)) {
            LOG.info("This URI is not resolvable as a codelist resource, wrong host.");
            throw new WebApplicationException("This URI is not resolvable as a codelist resource.");
        }
    }

    private URI parseUriFromString(final String uriString) {
        if (!uriString.isEmpty()) {
            return URI.create(uriString.replace(" ", "%20"));
        } else {
            LOG.info("URI string was not valid!");
            throw new WebApplicationException("URI string was not valid!");
        }
    }

    private String resolveResourceUrl(final String uriPath) {
        final String url;
        final String resourcePath = uriPath.substring(API_PATH_CODELIST.length() + 1);
        final List<String> resourceCodeValues = Arrays.asList(resourcePath.split("/"));
        if (!uriPath.toLowerCase().startsWith(API_PATH_CODELIST)) {
            LOG.info("Codelist resource URI not resolvable, wrong context path!");
            throw new WebApplicationException("Codelist resource URI not resolvable, wrong context path!");
        } else if (resourceCodeValues.isEmpty()) {
            LOG.info("Codelist resource URI not resolvable, empty resource path!");
            throw new WebApplicationException("Codelist resource URI not resolvable, empty resource path!");
        } else if (resourceCodeValues.size() == 1) {
            url = apiUtils.createCodeRegistryUrl(checkNotEmpty(resourceCodeValues.get(0)));
        } else if (resourceCodeValues.size() == 2) {
            url = apiUtils.createCodeSchemeUrl(checkNotEmpty(resourceCodeValues.get(0)), checkNotEmpty(resourceCodeValues.get(1)));
        } else if (resourceCodeValues.size() == 3) {
            url = apiUtils.createCodeUrl(checkNotEmpty(resourceCodeValues.get(0)), checkNotEmpty(resourceCodeValues.get(1)), checkNotEmpty(resourceCodeValues.get(2)));
        } else {
            LOG.info("Codelist resource URI not resolvable!");
            throw new WebApplicationException("Codelist resource URI not resolvable!");
        }
        return url;
    }

    private String checkNotEmpty(final String string) {
        if (string != null && !string.isEmpty()) {
            return string;
        } else {
            LOG.info("Resource hook not valid due to empty resource ID.");
            throw new WebApplicationException("Resource hook not valid due to empty resource ID.");
        }
    }
}
