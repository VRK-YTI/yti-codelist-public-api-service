package fi.vm.yti.codelist.api.resource;

import java.util.List;
import java.util.Set;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterInjector;

import fi.vm.yti.codelist.api.api.ApiUtils;
import fi.vm.yti.codelist.api.api.ResponseWrapper;
import fi.vm.yti.codelist.api.domain.Domain;
import fi.vm.yti.codelist.api.export.CodeExporter;
import fi.vm.yti.codelist.api.export.CodeRegistryExporter;
import fi.vm.yti.codelist.api.export.CodeSchemeExporter;
import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.CodeRegistryDTO;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ExternalReferenceDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;
import static java.util.Arrays.asList;

/**
 * REST resources for CodeRegistries, CodeSchemes and Codes.
 */
@Component
@Path("/v1/coderegistries")
@Api(value = "coderegistries")
@Produces({MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv"})
public class CodeRegistryResource extends AbstractBaseResource {

    private final ApiUtils apiUtils;
    private final Domain domain;
    private final CodeExporter codeExporter;
    private final CodeSchemeExporter codeSchemeExporter;
    private final CodeRegistryExporter codeRegistryExporter;

    @Inject
    public CodeRegistryResource(final ApiUtils apiUtils,
                                final Domain domain,
                                final CodeExporter codeExporter,
                                final CodeSchemeExporter codeSchemeExporter,
                                final CodeRegistryExporter codeRegistryExporter) {
        this.apiUtils = apiUtils;
        this.domain = domain;
        this.codeExporter = codeExporter;
        this.codeSchemeExporter = codeSchemeExporter;
        this.codeRegistryExporter = codeRegistryExporter;
    }

    @GET
    @ApiOperation(value = "Return a list of available CodeRegistries.", response = CodeRegistryDTO.class, responseContainer = "List")
    @ApiResponse(code = 200, message = "Returns all CodeRegistries in specified format.")
    @Produces({MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv"})
    public Response getCodeRegistries(@ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                      @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                      @ApiParam(value = "CodeRegistry CodeValue as string value.") @QueryParam("codeValue") final String codeRegistryCodeValue,
                                      @ApiParam(value = "CodeRegistry name as string value.") @QueryParam("name") final String name,
                                      @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                      @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                      @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                      @ApiParam(value = "Organizations filtering parameter, results will be registries belonging to these organizations") @QueryParam("organizations") final String organizationsCsv) {
        final List<String> organizations = organizationsCsv == null ? null : asList(organizationsCsv.split(","));
        if (FORMAT_CSV.equalsIgnoreCase(format)) {
            final Set<CodeRegistryDTO> codeRegistries = domain.getCodeRegistries(pageSize, from, codeRegistryCodeValue, name, Meta.parseAfterFromString(after), null, organizations);
            final String csv = codeRegistryExporter.createCsv(codeRegistries);
            return streamCsvCodeRegistriesOutput(csv);
        } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            final Set<CodeRegistryDTO> codeRegistries = domain.getCodeRegistries(pageSize, from, codeRegistryCodeValue, name, Meta.parseAfterFromString(after), null, organizations);
            final Workbook workbook = codeRegistryExporter.createExcel(codeRegistries, format);
            return streamExcelCodeRegistriesOutput(workbook);
        } else {
            final Meta meta = new Meta(200, null, null, after);
            ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODEREGISTRY, expand)));
            final Set<CodeRegistryDTO> codeRegistries = domain.getCodeRegistries(pageSize, from, codeRegistryCodeValue, name, meta.getAfter(), meta, organizations);
            meta.setResultCount(codeRegistries.size());
            final ResponseWrapper<CodeRegistryDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(codeRegistries);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}")
    @ApiOperation(value = "Return one specific CodeRegistry.", response = CodeRegistryDTO.class, responseContainer = "List")
    @ApiResponse(code = 200, message = "Returns one specific CodeRegistry in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getCodeRegistry(@ApiParam(value = "CodeRegistry CodeValue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                    @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                    @ApiParam(value = "Boolean that controls whether to embed CodeSchemes in payload or not.") @QueryParam("embedCodeSchemes") @DefaultValue("false") final Boolean embedCodeSchemes) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODEREGISTRY, expand)));
        final CodeRegistryDTO codeRegistry = domain.getCodeRegistry(codeRegistryCodeValue);
        if (codeRegistry != null) {
            if (embedCodeSchemes) {
                codeRegistry.setCodeSchemes(domain.getCodeSchemesByCodeRegistryCodeValue(codeRegistryCodeValue));
            }
            return Response.ok(codeRegistry).build();
        } else {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes")
    @ApiOperation(value = "Return CodeSchemeDTOs for a CodeRegistry.", response = CodeRegistryDTO.class, responseContainer = "List")
    @ApiResponse(code = 200, message = "Returns CodeSchemes for a CodeRegistry in specified format.")
    @Produces({MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv"})
    public Response getCodeRegistryCodeSchemes(@ApiParam(value = "CodeRegistry CodeValue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                               @ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                               @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                               @ApiParam(value = "Service classifications in CSL format.") @QueryParam("dataClassification") final String dataClassification,
                                               @ApiParam(value = "CodeRegistry PrefLabel as string value for searching.") @QueryParam("codeRegistryPrefLabel") final String codeRegistryPrefLabel,
                                               @ApiParam(value = "CodeScheme codeValue as string value for searching.") @QueryParam("codeValue") final String codeSchemeCodeValue,
                                               @ApiParam(value = "CodeScheme PrefLabel as string value for searching.") @QueryParam("prefLabel") final String codeSchemePrefLabel,
                                               @ApiParam(value = "Search term for matching codeValue and prefLabel.") @QueryParam("searchTerm") final String searchTerm,
                                               @ApiParam(value = "Status enumerations in CSL format.") @QueryParam("status") final String status,
                                               @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                               @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                               @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                               @ApiParam(value = "Sort mode for response values.") @QueryParam("sortMode") @DefaultValue("default") final String sortMode) {
        final Meta meta = new Meta(200, null, null, after);
        final List<String> dataClassificationList = parseDataClassifications(dataClassification);
        final List<String> statusList = parseStatus(status);
        final CodeRegistryDTO codeRegistry = domain.getCodeRegistry(codeRegistryCodeValue);
        if (codeRegistry != null) {
            if (FORMAT_CSV.equalsIgnoreCase(format.toLowerCase())) {
                final Set<CodeSchemeDTO> codeSchemes = domain.getCodeSchemes(pageSize, from, sortMode, null, codeRegistryCodeValue, codeRegistryPrefLabel, codeSchemeCodeValue, codeSchemePrefLabel, searchTerm, statusList, dataClassificationList, Meta.parseAfterFromString(after), null);
                final String csv = codeSchemeExporter.createCsv(codeSchemes);
                return streamCsvCodeSchemesOutput(csv);
            } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
                final Set<CodeSchemeDTO> codeSchemes = domain.getCodeSchemes(pageSize, from, sortMode, null, codeRegistryCodeValue, codeRegistryPrefLabel, codeSchemeCodeValue, codeSchemePrefLabel, searchTerm, statusList, dataClassificationList, Meta.parseAfterFromString(after), null);
                final Workbook workbook = codeSchemeExporter.createExcel(codeSchemes, format);
                return streamExcelCodeSchemesOutput(workbook);
            } else {
                ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODESCHEME, expand)));
                final Set<CodeSchemeDTO> codeSchemes = domain.getCodeSchemes(pageSize, from, sortMode, null, codeRegistryCodeValue, codeRegistryPrefLabel, codeSchemeCodeValue, codeSchemePrefLabel, searchTerm, statusList, dataClassificationList, meta.getAfter(), meta);
                meta.setResultCount(codeSchemes.size());
                final ResponseWrapper<CodeSchemeDTO> wrapper = new ResponseWrapper<>();
                wrapper.setResults(codeSchemes);
                wrapper.setMeta(meta);
                return Response.ok(wrapper).build();
            }
        } else {
            final ResponseWrapper<CodeSchemeDTO> wrapper = new ResponseWrapper<>();
            wrapper.setMeta(meta);
            meta.setCode(404);
            meta.setMessage("No such resource.");
            return Response.status(Response.Status.NOT_FOUND).entity(wrapper).build();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}")
    @ApiOperation(value = "Return one specific CodeScheme.", response = CodeSchemeDTO.class)
    @ApiResponse(code = 200, message = "Returns one specific CodeScheme in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getCodeRegistryCodeScheme(@ApiParam(value = "CodeRegistry CodeValue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                              @ApiParam(value = "CodeScheme CodeValue.", required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                              @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                              @ApiParam(value = "Boolean that controls whether to embed Codes in payload or not.") @QueryParam("embedCodes") @DefaultValue("false") final Boolean embedCodes) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODESCHEME, expand)));
        final CodeRegistryDTO codeRegistry = domain.getCodeRegistry(codeRegistryCodeValue);
        if (codeRegistry != null) {
            final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);
            if (codeScheme != null) {
                if (embedCodes) {
                    codeScheme.setCodes(domain.getCodesByCodeRegistryCodeValueAndCodeSchemeCodeValue(codeRegistryCodeValue, codeSchemeCodeValue));
                }
                return Response.ok(codeScheme).build();
            } else {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
        } else {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/codes")
    @ApiOperation(value = "Return Codes for a CodeScheme.", response = CodeDTO.class)
    @ApiResponse(code = 200, message = "Returns all Codes for CodeScheme in specified format.")
    @Produces({MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv"})
    public Response getCodeRegistryCodeSchemeCodes(@ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                                   @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                                   @ApiParam(value = "CodeRegistry CodeValue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                   @ApiParam(value = "CodeScheme CodeValue.", required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                   @ApiParam(value = "Code code.") @QueryParam("codeValue") final String codeCodeValue,
                                                   @ApiParam(value = "Code PrefLabel.") @QueryParam("prefLabel") final String prefLabel,
                                                   @ApiParam(value = "Code Broader Code Id.") @QueryParam("broaderCodeId") final String broaderCodeId,
                                                   @ApiParam(value = "Filter for hierarchy level.") @QueryParam("hierarchyLevel") final Integer hierarchyLevel,
                                                   @ApiParam(value = "Status enumerations in CSL format.") @QueryParam("status") final String status,
                                                   @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                                   @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                                   @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand) {
        final Meta meta = new Meta(Response.Status.OK.getStatusCode(), pageSize, from, after);
        final List<String> statusList = parseStatus(status);
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);
        if (codeScheme != null) {
            if (FORMAT_CSV.equalsIgnoreCase(format)) {
                final Set<CodeDTO> codes = domain.getCodes(pageSize, from, codeRegistryCodeValue, codeSchemeCodeValue, codeCodeValue, prefLabel, hierarchyLevel, broaderCodeId, statusList, Meta.parseAfterFromString(after), null);
                final String csv = codeExporter.createCsv(codes);
                return streamCsvCodesOutput(csv);
            } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
                final Set<CodeDTO> codes = domain.getCodes(pageSize, from, codeRegistryCodeValue, codeSchemeCodeValue, codeCodeValue, prefLabel, hierarchyLevel, broaderCodeId, statusList, Meta.parseAfterFromString(after), null);
                final Workbook workbook = codeExporter.createExcel(codes, format);
                return streamExcelCodesOutput(workbook);
            } else {
                ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODE, expand)));
                final Set<CodeDTO> codes = domain.getCodes(pageSize, from, codeRegistryCodeValue, codeSchemeCodeValue, codeCodeValue, prefLabel, hierarchyLevel, broaderCodeId, statusList, meta.getAfter(), meta);
                if (pageSize != null && from + pageSize < meta.getTotalResults()) {
                    meta.setNextPage(apiUtils.createNextPageUrl(API_VERSION, API_PATH_CODEREGISTRIES + "/" + codeRegistryCodeValue + API_PATH_CODESCHEMES + "/" + codeSchemeCodeValue + API_PATH_CODES, after, pageSize, from + pageSize));
                }
                final ResponseWrapper<CodeDTO> wrapper = new ResponseWrapper<>();
                wrapper.setMeta(meta);
                if (codes == null) {
                    meta.setCode(404);
                    meta.setMessage("No such resource.");
                    return Response.status(Response.Status.NOT_FOUND).entity(wrapper).build();
                }
                wrapper.setResults(codes);
                return Response.ok(wrapper).build();
            }
        } else {
            final ResponseWrapper<CodeSchemeDTO> wrapper = new ResponseWrapper<>();
            wrapper.setMeta(meta);
            meta.setCode(404);
            meta.setMessage("No such resource.");
            return Response.status(Response.Status.NOT_FOUND).entity(wrapper).build();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/externalreferences")
    @ApiOperation(value = "Return Codes for a CodeSchemeO.", response = CodeDTO.class)
    @ApiResponse(code = 200, message = "Returns all Codes for CodeScheme in specified format.")
    @Produces({MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv"})
    public Response getCodeRegistryCodeSchemeExternalReferences(@ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                                                @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                                                @ApiParam(value = "CodeRegistry CodeValue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                                @ApiParam(value = "CodeScheme CodeValue.", required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                                @ApiParam(value = "ExternalReference PrefLabel.") @QueryParam("prefLabel") final String prefLabel,
                                                                @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                                                @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand) {
        final Meta meta = new Meta(Response.Status.OK.getStatusCode(), pageSize, from, after);
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);
        if (codeScheme != null) {
            ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODE, expand)));
            final Set<ExternalReferenceDTO> externalReferences = domain.getExternalReferences(pageSize, from, prefLabel, codeScheme, meta.getAfter(), meta);
            if (pageSize != null && from + pageSize < meta.getTotalResults()) {
                meta.setNextPage(apiUtils.createNextPageUrl(API_VERSION, API_PATH_CODEREGISTRIES + "/" + codeRegistryCodeValue + API_PATH_CODESCHEMES + "/" + codeSchemeCodeValue + API_PATH_EXTERNALREFERENCES, after, pageSize, from + pageSize));
            }
            final ResponseWrapper<ExternalReferenceDTO> wrapper = new ResponseWrapper<>();
            wrapper.setMeta(meta);
            if (externalReferences == null) {
                meta.setCode(404);
                meta.setMessage("No such resource.");
                return Response.status(Response.Status.NOT_FOUND).entity(wrapper).build();
            }
            wrapper.setResults(externalReferences);
            return Response.ok(wrapper).build();
        } else {
            final ResponseWrapper<CodeSchemeDTO> wrapper = new ResponseWrapper<>();
            wrapper.setMeta(meta);
            meta.setCode(404);
            meta.setMessage("No such resource.");
            return Response.status(Response.Status.NOT_FOUND).entity(wrapper).build();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/codes/{codeCodeValue}")
    @ApiOperation(value = "Return one code from specific codescheme under specific coderegistry.", response = CodeDTO.class)
    @ApiResponse(code = 200, message = "Returns one registeritem from specific register in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getCodeRegistryCodeSchemeCode(@ApiParam(value = "CodeRegistry CodeValue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                  @ApiParam(value = "CodeScheme CodeValue.", required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                  @ApiParam(value = "Code code.", required = true) @PathParam("codeCodeValue") final String codeCodeValue,
                                                  @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODE, expand)));
        final CodeDTO code = domain.getCode(codeRegistryCodeValue, codeSchemeCodeValue, codeCodeValue);
        if (code == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(code).build();
    }
}
