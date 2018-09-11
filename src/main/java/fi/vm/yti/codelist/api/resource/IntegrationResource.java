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

import fi.vm.yti.codelist.api.api.ResponseWrapper;
import fi.vm.yti.codelist.api.domain.Domain;
import fi.vm.yti.codelist.api.dto.ResourceDTO;
import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import static fi.vm.yti.codelist.common.constants.ApiConstants.FILTER_NAME_RESOURCE;

@Component
@Path("/v1/integration")
@Api(value = "integration")
@Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8" })
public class IntegrationResource extends AbstractBaseResource {

    private final Domain domain;

    @Inject
    public IntegrationResource(final Domain domain) {
        this.domain = domain;
    }

    @GET
    @Path("/containers")
    @ApiOperation(value = "Return one specific Code.", response = CodeDTO.class)
    @ApiResponse(code = 200, message = "Returns one specific Code in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getContainers(@ApiParam(value = "Language code for sorting results.") @QueryParam("language") @DefaultValue("fi") final String language,
                                  @ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                  @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                  @ApiParam(value = "Status enumerations in CSL format.") @QueryParam("status") final String status,
                                  @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_RESOURCE)));
        final Meta meta = new Meta(200, null, null, after);
        final List<String> statusList = parseStatus(status);
        final Set<ResourceDTO> containers = domain.getContainers(pageSize, from, language, statusList, Meta.parseAfterFromString(after), meta);
        meta.setResultCount(containers.size());
        final ResponseWrapper<ResourceDTO> wrapper = new ResponseWrapper<>();
        wrapper.setResults(containers);
        wrapper.setMeta(meta);
        return Response.ok(wrapper).build();
    }

    @GET
    @Path("/resources")
    @ApiOperation(value = "Return one specific Code.", response = CodeDTO.class)
    @ApiResponse(code = 200, message = "Returns one specific Code in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getResources(@ApiParam(value = "Language code for sorting results.") @QueryParam("language") @DefaultValue("fi") final String language,
                                 @ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                 @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                 @ApiParam(value = "Status enumerations in CSL format.") @QueryParam("status") final String status,
                                 @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                 @ApiParam(value = "Container URI.", required = true) @QueryParam("uri") final String codeSchemeUri) {
        final URI resolveUri = parseUriFromString(codeSchemeUri);
        ensureSuomiFiUriHost(resolveUri.getHost());
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_RESOURCE)));
        final List<String> statusList = parseStatus(status);
        final Meta meta = new Meta(200, null, null, after);
        final Set<ResourceDTO> resources = domain.getResources(pageSize, from, codeSchemeUri, language, statusList, meta.getAfter(), meta);
        meta.setResultCount(resources.size());
        final ResponseWrapper<ResourceDTO> wrapper = new ResponseWrapper<>();
        wrapper.setResults(resources);
        wrapper.setMeta(meta);
        return Response.ok(wrapper).build();
    }
}
