package fi.vm.yti.codelist.api.resource;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.jackson.internal.jackson.jaxrs.cfg.ObjectWriterInjector;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import fi.vm.yti.codelist.api.api.ApiUtils;
import fi.vm.yti.codelist.api.api.ResponseWrapper;
import fi.vm.yti.codelist.api.domain.Domain;
import fi.vm.yti.codelist.api.dto.IntegrationResourceRequestDTO;
import fi.vm.yti.codelist.api.dto.ResourceDTO;
import fi.vm.yti.codelist.api.exception.YtiCodeListException;
import fi.vm.yti.codelist.common.dto.ErrorModel;
import fi.vm.yti.codelist.common.dto.Meta;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;
import static java.util.Arrays.asList;

@Component
@Path("/v1/integration")
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
    @Operation(description = "API for fetching container resources")
    @ApiResponse(responseCode = "200", description = "Returns container resources with meta element that shows details and a results list.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getContainers(@Parameter(description = "Language code for sorting results.", in = ParameterIn.QUERY) @QueryParam("language") @DefaultValue("fi") final String language,
                                  @Parameter(description = "Pagination parameter for page size.", in = ParameterIn.QUERY) @QueryParam("pageSize") final Integer pageSize,
                                  @Parameter(description = "Pagination parameter for start index.", in = ParameterIn.QUERY) @QueryParam("from") @DefaultValue("0") final Integer from,
                                  @Parameter(description = "Status enumerations in CSL format.", in = ParameterIn.QUERY) @QueryParam("status") final String status,
                                  @Parameter(description = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("after") final String after,
                                  @Parameter(description = "Search term used to filter results based on partial prefLabel or codeValue match.", in = ParameterIn.QUERY) @QueryParam("searchTerm") final String searchTerm,
                                  @Parameter(description = "User organizations filtering parameter, for filtering incomplete code schemes", in = ParameterIn.QUERY) @QueryParam("includeIncompleteFrom") final String includeIncompleteFrom,
                                  @Parameter(description = "User organizations filtering parameter, for filtering incomplete code schemes", in = ParameterIn.QUERY) @QueryParam("includeIncomplete") @DefaultValue("false") final Boolean includeIncomplete,
                                  @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(), pretty));
        final Meta meta = new Meta(200, pageSize, from, after);
        final List<String> statusList = parseStatus(status);
        final List<String> includeIncompleteFromList = includeIncompleteFrom == null ? null : asList(includeIncompleteFrom.toLowerCase().split(","));
        final Set<ResourceDTO> containers = domain.getContainers(pageSize, from, language, statusList, searchTerm, null, includeIncompleteFromList, includeIncomplete, meta);
        meta.setResultCount(containers.size());
        if (pageSize != null && from + pageSize < meta.getTotalResults()) {
            meta.setNextPage(apiUtils.createNextPageUrl(API_VERSION, API_PATH_INTEGRATION + API_PATH_CONTAINERS, after, pageSize, from + pageSize));
        }
        final ResponseWrapper<ResourceDTO> wrapper = new ResponseWrapper<>();
        wrapper.setResults(containers);
        wrapper.setMeta(meta);
        return Response.ok(wrapper).build();
    }

    @POST
    @Path("/containers")
    @Operation(description = "API for fetching container resources")
    @ApiResponse(responseCode = "200", description = "Returns container resources with meta element that shows details and a results list.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getContainers(@Parameter(description = "Integration resource request parameters as JSON payload.") @RequestBody final String integrationRequestData) {
        final IntegrationResourceRequestDTO request = parseIntegrationRequestDto(integrationRequestData);
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(), request.getPretty()));
        final List<String> statusList = request.getStatus();
        final List<String> filter = request.getFilter();
        final Set<String> excludedUrisSet;
        if (filter != null) {
            excludedUrisSet = new HashSet<>(request.getFilter());
        } else {
            excludedUrisSet = null;
        }
        final List<String> includeIncompleteFromList = request.getIncludeIncompleteFrom();
        final Integer pageSize = request.getPageSize();
        final Integer from = request.getPageFrom();
        final String after = request.getAfter();
        final String searchTerm = request.getSearchTerm();
        final String language = request.getLanguage();
        final boolean includeIncomplete = request.getIncludeIncomplete();
        final Meta meta = new Meta(200, pageSize, from, after);
        final Set<ResourceDTO> containers = domain.getContainers(pageSize, from, language, statusList, searchTerm, excludedUrisSet, includeIncompleteFromList, includeIncomplete, meta);
        meta.setResultCount(containers.size());
        if (pageSize != null && from + pageSize < meta.getTotalResults()) {
            meta.setNextPage(apiUtils.createNextPageUrl(API_VERSION, API_PATH_INTEGRATION + API_PATH_CONTAINERS, after, pageSize, from + pageSize));
        }
        final ResponseWrapper<ResourceDTO> wrapper = new ResponseWrapper<>();
        wrapper.setResults(containers);
        wrapper.setMeta(meta);
        return Response.ok(wrapper).build();
    }

    @GET
    @Path("/resources")
    @Operation(description = "API for fetching resources for a container")
    @ApiResponse(responseCode = "200", description = "Returns resources for a specific container with meta element that shows details and a results list.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getResources(@Parameter(description = "Language code for sorting results.", in = ParameterIn.QUERY) @QueryParam("language") @DefaultValue("fi") final String language,
                                 @Parameter(description = "Pagination parameter for page size.", in = ParameterIn.QUERY) @QueryParam("pageSize") final Integer pageSize,
                                 @Parameter(description = "Pagination parameter for start index.", in = ParameterIn.QUERY) @QueryParam("from") @DefaultValue("0") final Integer from,
                                 @Parameter(description = "Status enumerations in CSL format.", in = ParameterIn.QUERY) @QueryParam("status") final String status,
                                 @Parameter(description = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("after") final String after,
                                 @Parameter(description = "Container URI.", in = ParameterIn.QUERY) @QueryParam("container") final String containerUri,
                                 @Parameter(description = "Search term used to filter results based on partial prefLabel or codeValue match.", in = ParameterIn.QUERY) @QueryParam("searchTerm") final String searchTerm,
                                 @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        if (containerUri != null) {
            final URI resolveUri = parseUriFromString(containerUri);
            ensureSuomiFiUriHost(resolveUri.getHost());
        }
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(), pretty));
        final List<String> statusList = parseStatus(status);
        final Meta meta = new Meta(200, pageSize, from, after);
        final Set<ResourceDTO> resources = domain.getResources(pageSize, from, containerUri, language, statusList, searchTerm, null, meta);
        meta.setResultCount(resources.size());
        if (pageSize != null && from + pageSize < meta.getTotalResults()) {
            if (containerUri != null) {
                meta.setNextPage(apiUtils.createNextPageUrl(API_VERSION, API_PATH_INTEGRATION + API_PATH_RESOURCES, after, pageSize, from + pageSize) + "&container=" + containerUri);
            }
            meta.setNextPage(apiUtils.createNextPageUrl(API_VERSION, API_PATH_INTEGRATION + API_PATH_RESOURCES, after, pageSize, from + pageSize) + "&container=" + containerUri);
        }
        final ResponseWrapper<ResourceDTO> wrapper = new ResponseWrapper<>();
        wrapper.setResults(resources);
        wrapper.setMeta(meta);
        return Response.ok(wrapper).build();
    }

    @POST
    @Path("/resources")
    @Operation(description = "API for fetching resources for a container")
    @ApiResponse(responseCode = "200", description = "Returns resources for a specific container with meta element that shows details and a results list.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getResources(@Parameter(description = "Integration resource request parameters as JSON payload.") @RequestBody final String integrationRequestData) {
        final IntegrationResourceRequestDTO request = parseIntegrationRequestDto(integrationRequestData);
        final String container = request.getContainer();
        if (container != null) {
            final URI containerUri = parseUriFromString(container);
            ensureSuomiFiUriHost(containerUri.getHost());
        }
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(), request.getPretty()));
        final List<String> statusList = request.getStatus();
        final List<String> filter = request.getFilter();
        final Set<String> excludedUrisSet;
        if (filter != null) {
            excludedUrisSet = new HashSet<>(filter);
        } else {
            excludedUrisSet = null;
        }
        final Integer pageSize = request.getPageSize();
        final Integer from = request.getPageFrom();
        final String after = request.getAfter();
        final String language = request.getLanguage();
        final String searchTerm = request.getSearchTerm();
        final Meta meta = new Meta(200, pageSize, from, after);
        final Set<ResourceDTO> resources = domain.getResources(pageSize, from, container, language, statusList, searchTerm, excludedUrisSet, meta);
        meta.setResultCount(resources.size());
        final ResponseWrapper<ResourceDTO> wrapper = new ResponseWrapper<>();
        wrapper.setResults(resources);
        wrapper.setMeta(meta);
        return Response.ok(wrapper).build();
    }

    private IntegrationResourceRequestDTO parseIntegrationRequestDto(final String integrationRequestData) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(integrationRequestData, new TypeReference<IntegrationResourceRequestDTO>() {
            });
        } catch (IOException e) {
            throw new YtiCodeListException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), "Malformed resources request body!"));
        }
    }
}
