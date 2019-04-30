package fi.vm.yti.codelist.api.resource;

import java.net.URI;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterInjector;

import fi.vm.yti.codelist.api.api.ApiUtils;
import fi.vm.yti.codelist.api.api.ResponseWrapper;
import fi.vm.yti.codelist.api.domain.Domain;
import fi.vm.yti.codelist.api.dto.ResourceDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
@Path("/v1/integration")
@Api(value = "integration")
@Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8" })
public class IntegrationResource extends AbstractBaseResource {

    private final Domain domain;
    private final ApiUtils apiUtils;

    @Inject
    public IntegrationResource(final Domain domain,
                               final ApiUtils apiUtils) {
        this.domain = domain;
        this.apiUtils = apiUtils;
    }

    @GET
    @Path("/containers")
    @ApiOperation(value = "Return a list of container resources.")
    @ApiResponse(code = 200, message = "Returns one specific Code in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getContainers(@ApiParam(value = "Language code for sorting results.") @QueryParam("language") @DefaultValue("fi") final String language,
                                  @ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                  @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                  @ApiParam(value = "Status enumerations in CSL format.") @QueryParam("status") final String status,
                                  @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                  @ApiParam(value = "Include pagination related meta element and wrap response items in bulk array.") @QueryParam("includeMeta") @DefaultValue("false") final boolean includeMeta,
                                  @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_RESOURCE), pretty));
        final Meta meta = new Meta(200, pageSize, from, after);
        final List<String> statusList = parseStatus(status);
        final Set<ResourceDTO> containers = domain.getContainers(pageSize, from, language, statusList, meta.getAfter(), meta);
        if (includeMeta) {
            meta.setResultCount(containers.size());
            if (pageSize != null && from + pageSize < meta.getTotalResults()) {
                meta.setNextPage(apiUtils.createNextPageUrl(API_VERSION, API_PATH_INTEGRATION + API_PATH_CONTAINERS, after, pageSize, from + pageSize) + "&includeMeta=true");
            }
            final ResponseWrapper<ResourceDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(containers);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        } else {
            return Response.ok(containers).build();
        }
    }

    @GET
    @Path("/resources")
    @ApiOperation(value = "Return a list of available resources for one container.")
    @ApiResponse(code = 200, message = "Returns one specific Code in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getResources(@ApiParam(value = "Language code for sorting results.") @QueryParam("language") @DefaultValue("fi") final String language,
                                 @ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                 @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                 @ApiParam(value = "Status enumerations in CSL format.") @QueryParam("status") final String status,
                                 @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                 @ApiParam(value = "Container URI.", required = true) @QueryParam("uri") final String codeSchemeUri,
                                 @ApiParam(value = "Include pagination related meta element and wrap response items in bulk array.") @QueryParam("includeMeta") @DefaultValue("false") final boolean includeMeta,
                                 @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
        final URI resolveUri = parseUriFromString(codeSchemeUri);
        ensureSuomiFiUriHost(resolveUri.getHost());
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_RESOURCE), pretty));
        final List<String> statusList = parseStatus(status);
        final Meta meta = new Meta(200, pageSize, from, after);
        final Set<ResourceDTO> resources = domain.getResources(pageSize, from, codeSchemeUri, language, statusList, meta.getAfter(), meta);
        if (includeMeta) {
            meta.setResultCount(resources.size());
            if (pageSize != null && from + pageSize < meta.getTotalResults()) {
                meta.setNextPage(apiUtils.createNextPageUrl(API_VERSION, API_PATH_INTEGRATION + API_PATH_RESOURCES, after, pageSize, from + pageSize) + "&includeMeta=true&uri=" + codeSchemeUri);
            }
            final ResponseWrapper<ResourceDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(resources);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        } else {
            return Response.ok(resources).build();
        }
    }
}
