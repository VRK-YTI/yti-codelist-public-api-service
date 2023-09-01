package fi.vm.yti.codelist.api.resource;

import java.util.*;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.poi.ss.usermodel.Workbook;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.cfg.ObjectWriterInjector;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import fi.vm.yti.codelist.api.api.ApiUtils;
import fi.vm.yti.codelist.api.api.ResponseWrapper;
import fi.vm.yti.codelist.api.domain.Domain;
import fi.vm.yti.codelist.api.exception.NotFoundException;
import fi.vm.yti.codelist.api.export.CodeExporter;
import fi.vm.yti.codelist.api.export.CodeRegistryExporter;
import fi.vm.yti.codelist.api.export.CodeSchemeExporter;
import fi.vm.yti.codelist.api.export.ExtensionExporter;
import fi.vm.yti.codelist.api.export.MemberExporter;
import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.CodeRegistryDTO;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ExtensionDTO;
import fi.vm.yti.codelist.common.dto.ExternalReferenceDTO;
import fi.vm.yti.codelist.common.dto.MemberDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import fi.vm.yti.codelist.common.model.CodeSchemeListItem;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import static fi.vm.yti.codelist.api.util.EncodingUtils.urlDecodeCodeValue;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;
import static java.util.Arrays.asList;

@Component
@Path("/v1/coderegistries")
@Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "text/csv" })
public class CodeRegistryResource extends AbstractBaseResource {

    private static final String HEADER_CONTENT_DISPOSITION = "content-disposition";
    private final ApiUtils apiUtils;
    private final Domain domain;
    private final CodeExporter codeExporter;
    private final CodeSchemeExporter codeSchemeExporter;
    private final CodeRegistryExporter codeRegistryExporter;
    private final ExtensionExporter extensionExporter;
    private final MemberExporter memberExporter;

    @Inject
    public CodeRegistryResource(final ApiUtils apiUtils,
                                final Domain domain,
                                final CodeExporter codeExporter,
                                final CodeSchemeExporter codeSchemeExporter,
                                final CodeRegistryExporter codeRegistryExporter,
                                final ExtensionExporter extensionExporter,
                                final MemberExporter memberExporter) {
        this.apiUtils = apiUtils;
        this.domain = domain;
        this.codeExporter = codeExporter;
        this.codeSchemeExporter = codeSchemeExporter;
        this.codeRegistryExporter = codeRegistryExporter;
        this.extensionExporter = extensionExporter;
        this.memberExporter = memberExporter;
    }

    @GET
    @Operation(summary = "Return a list of available CodeRegistries.")
    @ApiResponse(responseCode = "200", description = "Returns all CodeRegistries in specified format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "text/csv" })
    @Tag(name = "CodeRegistry")
    public Response getCodeRegistries(@Parameter(description = "Pagination parameter for page size.", in = ParameterIn.QUERY) @QueryParam("pageSize") final Integer pageSize,
                                      @Parameter(description = "Pagination parameter for start index.", in = ParameterIn.QUERY) @QueryParam("from") @DefaultValue("0") final Integer from,
                                      @Parameter(description = "CodeRegistry CodeValue as string value.", in = ParameterIn.QUERY) @QueryParam("codeValue") final String codeRegistryCodeValue,
                                      @Parameter(description = "CodeRegistry name as string value.", in = ParameterIn.QUERY) @QueryParam("name") final String name,
                                      @Parameter(description = "Format for content.", in = ParameterIn.QUERY) @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                      @Parameter(description = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("after") final String after,
                                      @Parameter(description = "Before date filtering parameter, results will be codes with modified date before this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("before") final String before,
                                      @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                      @Parameter(description = "Organizations filtering parameter, results will be registries belonging to these organizations", in = ParameterIn.QUERY) @QueryParam("organizations") final String organizationsCsv,
                                      @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        final List<String> organizations = organizationsCsv == null ? null : asList(organizationsCsv.split(","));
        final Meta meta = new Meta(200, pageSize, from, parseDateFromString(after), parseDateFromString(before));
        final Set<CodeRegistryDTO> codeRegistries = domain.getCodeRegistries(codeRegistryCodeValue, name, meta, organizations);
        if (FORMAT_CSV.equalsIgnoreCase(format)) {
            final String csv = codeRegistryExporter.createCsv(codeRegistries);
            return streamCsvCodeRegistriesOutput(csv);
        } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            final Workbook workbook = codeRegistryExporter.createExcel(codeRegistries, format);
            return streamExcelCodeRegistriesOutput(workbook);
        } else {
            ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODEREGISTRY, expand), pretty));
            final ResponseWrapper<CodeRegistryDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(codeRegistries);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}")
    @Operation(description = "Return one specific CodeRegistry.")
    @ApiResponse(responseCode = "200", description = "Returns one specific CodeRegistry in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    @Tag(name = "CodeRegistry")
    public Response getCodeRegistry(@Parameter(description = "CodeRegistry CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                    @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                    @Parameter(description = "Language code for sorting results.", in = ParameterIn.QUERY) @QueryParam("language") @DefaultValue("fi") final String language,
                                    @Parameter(description = "Boolean that controls whether to embed CodeSchemes in payload or not.", in = ParameterIn.QUERY) @QueryParam("embedCodeSchemes") @DefaultValue("false") final boolean embedCodeSchemes,
                                    @Parameter(description = "User organizations filtering parameter, for filtering unfinished code schemes") @QueryParam("userOrganizations") final String userOrganizationsCsv,
                                    @Parameter(description = "Include INCOMPLETE statused code schemes.", in = ParameterIn.QUERY) @QueryParam("includeIncomplete") @DefaultValue("false") final boolean includeIncomplete,
                                    @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODEREGISTRY, expand), pretty));
        final List<String> userOrganizations = userOrganizationsCsv == null ? null : asList(userOrganizationsCsv.toLowerCase().split(","));
        final CodeRegistryDTO codeRegistry = domain.getCodeRegistry(codeRegistryCodeValue);
        if (codeRegistry != null) {
            if (embedCodeSchemes) {
                codeRegistry.setCodeSchemes(domain.getCodeSchemesByCodeRegistryCodeValue(codeRegistryCodeValue, null, userOrganizations, includeIncomplete, language));
            }
            return Response.ok(codeRegistry).build();
        } else {
            throw new NotFoundException();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/")
    @Operation(description = "Return CodeSchemes for a CodeRegistry.")
    @ApiResponse(responseCode = "200", description = "Returns CodeSchemes for a CodeRegistry in specified format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "text/csv" })
    @Tag(name = "CodeScheme")
    public Response getCodeRegistryCodeSchemes(@Parameter(description = "CodeRegistry CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                               @Parameter(description = "Pagination parameter for page size.", in = ParameterIn.QUERY) @QueryParam("pageSize") final Integer pageSize,
                                               @Parameter(description = "Pagination parameter for start index.", in = ParameterIn.QUERY) @QueryParam("from") @DefaultValue("0") final Integer from,
                                               @Parameter(description = "Service information domain classifications in CSL format.", in = ParameterIn.QUERY) @QueryParam("infoDomain") final String infoDomain,
                                               @Parameter(description = "CodeRegistry PrefLabel as string value for searching.", in = ParameterIn.QUERY) @QueryParam("codeRegistryPrefLabel") final String codeRegistryPrefLabel,
                                               @Parameter(description = "CodeScheme codeValue as string value for searching.", in = ParameterIn.QUERY) @QueryParam("codeValue") final String codeSchemeCodeValue,
                                               @Parameter(description = "CodeScheme PrefLabel as string value for searching.", in = ParameterIn.QUERY) @QueryParam("prefLabel") final String codeSchemePrefLabel,
                                               @Parameter(description = "Language code for sorting results.", in = ParameterIn.QUERY) @QueryParam("language") @DefaultValue("fi") final String language,
                                               @Parameter(description = "Search term for matching codeValue and prefLabel.", in = ParameterIn.QUERY) @QueryParam("searchTerm") final String searchTerm,
                                               @Parameter(description = "Status enumerations in CSL format.", in = ParameterIn.QUERY) @QueryParam("status") final String status,
                                               @Parameter(description = "Extension PropertyType localName as string value for searching.", in = ParameterIn.QUERY) @QueryParam("extensionPropertyType") final String extensionPropertyType,
                                               @Parameter(description = "Format for content.", in = ParameterIn.QUERY) @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                               @Parameter(description = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("after") final String after,
                                               @Parameter(description = "Before date filtering parameter, results will be codes with modified date before this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("before") final String before,
                                               @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                               @Parameter(description = "Sort mode for response values.", in = ParameterIn.QUERY) @QueryParam("sortMode") @DefaultValue("default") final String sortMode,
                                               @Parameter(description = "User organizations filtering parameter, for filtering unfinished code schemes") @QueryParam("userOrganizations") final String userOrganizationsCsv,
                                               @Parameter(description = "Include INCOMPLETE statused code schemes.", in = ParameterIn.QUERY) @QueryParam("includeIncomplete") @DefaultValue("false") final boolean includeIncomplete,
                                               @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        final Meta meta = new Meta(200, pageSize, from, parseDateFromString(after), parseDateFromString(before));
        final List<String> userOrganizations = userOrganizationsCsv == null ? null : asList(userOrganizationsCsv.toLowerCase().split(","));
        final List<String> infoDomainsList = parseInfoDomainsCls(infoDomain);
        final List<String> statusList = parseStatusCsl(status);
        final CodeRegistryDTO codeRegistry = domain.getCodeRegistry(codeRegistryCodeValue);
        if (codeRegistry != null) {
            final Set<CodeSchemeDTO> codeSchemes = domain.getCodeSchemes(sortMode, null, userOrganizations, includeIncomplete, codeRegistryCodeValue, codeRegistryPrefLabel, codeSchemeCodeValue, codeSchemePrefLabel, language, searchTerm, false, false, statusList, infoDomainsList, extensionPropertyType, meta);
            if (FORMAT_CSV.equalsIgnoreCase(format.toLowerCase())) {
                final String csv = codeSchemeExporter.createCsv(codeSchemes);
                return streamCsvCodeSchemesOutput(csv);
            } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
                final Workbook workbook = codeSchemeExporter.createExcel(codeSchemes, format);
                return streamExcelCodeSchemesOutput(workbook);
            } else {
                ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODESCHEME, expand), pretty));
                final ResponseWrapper<CodeSchemeDTO> wrapper = new ResponseWrapper<>();
                wrapper.setResults(codeSchemes);
                wrapper.setMeta(meta);
                return Response.ok(wrapper).build();
            }
        } else {
            throw new NotFoundException();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}")
    @Operation(description = "Return one specific CodeScheme.")
    @ApiResponse(responseCode = "200", description = "Returns one specific CodeScheme in JSON format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", MediaType.TEXT_PLAIN + ";charset=UTF-8", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "text/csv" })
    @Tag(name = "CodeScheme")
    public Response getCodeRegistryCodeScheme(@Parameter(description = "CodeRegistry CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                              @Parameter(description = "CodeScheme CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                              @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                              @Parameter(description = "Format for content.", in = ParameterIn.QUERY) @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                              @Parameter(description = "Download JSON as a file.", in = ParameterIn.QUERY) @QueryParam("downloadFile") @DefaultValue("false") final boolean downloadFile,
                                              @Parameter(description = "Boolean that controls whether to embed Codes in the payload or not.", in = ParameterIn.QUERY) @QueryParam("embedCodes") @DefaultValue("false") final boolean embedCodes,
                                              @Parameter(description = "Boolean that controls whether to embed Extensions in the payload or not.", in = ParameterIn.QUERY) @QueryParam("embedExtensions") @DefaultValue("false") final boolean embedExtensions,
                                              @Parameter(description = "Boolean that controls whether to embed embedMembers in the payload or not.", in = ParameterIn.QUERY) @QueryParam("embedMembers") @DefaultValue("false") final boolean embedMembers,
                                              @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODESCHEME, expand), pretty));
        final CodeRegistryDTO codeRegistry = domain.getCodeRegistry(codeRegistryCodeValue);
        if (codeRegistry != null) {
            if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
                final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);
                if (codeScheme != null) {
                    final Workbook workbook = codeSchemeExporter.createExcel(codeScheme, format);
                    return streamExcelCodeSchemeOutput(workbook, "codelist_" + codeScheme.getCodeValue());
                } else {
                    throw new NotFoundException();
                }
            } else if (FORMAT_CSV.equalsIgnoreCase(format)) {
                final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);
                if (codeScheme != null) {
                    final String csv = codeSchemeExporter.createCsv(codeScheme);
                    return streamCsvCodeSchemeOutput(csv, "codelist_" + codeScheme.getCodeValue());
                } else {
                    throw new NotFoundException();
                }
            } else {
                final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);
                if (codeScheme != null) {
                    if (embedCodes) {
                        final Set<CodeDTO> codes = domain.getCodesByCodeRegistryCodeValueAndCodeSchemeCodeValue(codeRegistryCodeValue, codeSchemeCodeValue);
                        filterCodes(codes);
                        codeScheme.setCodes(codes);
                    }
                    if (embedExtensions) {
                        Set<ExtensionDTO> extensions = domain.getExtensions(codeScheme);
                        filterExtensions(extensions);
                        if (embedMembers) {
                            for (ExtensionDTO extension : extensions) {
                                final Set<MemberDTO> members = domain.getMembers(extension, null);
                                filterMembers(members);
                                extension.setMembers(members);
                            }
                        }
                        codeScheme.setExtensions(extensions);
                    }
                    Response response = Response.ok(codeScheme).build();
                    if (downloadFile) {
                        response.getHeaders().putSingle(HEADER_CONTENT_DISPOSITION, "attachment; filename = " + "codelist_" + codeScheme.getCodeValue() + ".json");
                    }
                    response.getHeaders().putSingle("Content-Type", MediaType.APPLICATION_JSON + ";charset=utf-8");
                    return response;
                } else {
                    throw new NotFoundException();
                }
            }
        } else {
            throw new NotFoundException();
        }
    }


    @GET
    @Path("codes")
    public Set<CodeDTO> getFromList(@QueryParam("uri") List<String> uriList) {
        List<String> statuses = List.of();
        final Meta meta = new Meta(200, null, 0, parseDateFromString(null), parseDateFromString(null));
        var codes = new HashSet<CodeDTO>();
        uriList.forEach(uri -> {
            var splitUri = uri.split("/");
            ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODE, null), null));
            var dtos = domain.getCodes(splitUri[splitUri.length-2], splitUri[splitUri.length-1], null, null, null, null, null, statuses, meta);
            codes.addAll(dtos);
        });
        return codes;
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/codes/")
    @Operation(description = "Return Codes for a CodeScheme.")
    @ApiResponse(responseCode = "200", description = "Returns all Codes for CodeScheme in specified format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "text/csv" })
    @Tag(name = "Code")
    public Response getCodeRegistryCodeSchemeCodes(@Parameter(description = "Pagination parameter for page size.", in = ParameterIn.QUERY) @QueryParam("pageSize") final Integer pageSize,
                                                   @Parameter(description = "Pagination parameter for start index.", in = ParameterIn.QUERY) @QueryParam("from") @DefaultValue("0") final Integer from,
                                                   @Parameter(description = "CodeRegistry CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                   @Parameter(description = "CodeScheme CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                   @Parameter(description = "Code code.", in = ParameterIn.QUERY) @QueryParam("codeValue") final String codeCodeValue,
                                                   @Parameter(description = "Code PrefLabel.", in = ParameterIn.QUERY) @QueryParam("prefLabel") final String prefLabel,
                                                   @Parameter(description = "Code Broader Code Id.", in = ParameterIn.QUERY) @QueryParam("broaderCodeId") final String broaderCodeId,
                                                   @Parameter(description = "Filter for hierarchy level.", in = ParameterIn.QUERY) @QueryParam("hierarchyLevel") final Integer hierarchyLevel,
                                                   @Parameter(description = "Status enumerations in CSL format.", in = ParameterIn.QUERY) @QueryParam("status") final String status,
                                                   @Parameter(description = "Format for content.", in = ParameterIn.QUERY) @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                                   @Parameter(description = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("after") final String after,
                                                   @Parameter(description = "Before date filtering parameter, results will be codes with modified date before this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("before") final String before,
                                                   @Parameter(description = "Language code for sorting results.", in = ParameterIn.QUERY) @QueryParam("language") final String language,
                                                   @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                                   @Parameter(description = "Returns code codeValues in JSON array format") @QueryParam("array") final String array,
                                                   @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty,
                                                   @Parameter(description = "True if the JSON array should be downloaded as file", in = ParameterIn.QUERY) @QueryParam("downloadArray") final boolean downloadArray) {
        final Meta meta = new Meta(200, pageSize, from, parseDateFromString(after), parseDateFromString(before));
        final List<String> statusList = parseStatusCsl(status);
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);
        if (codeScheme != null) {
            final Set<CodeDTO> codes = domain.getCodes(codeRegistryCodeValue, codeSchemeCodeValue, codeCodeValue, prefLabel, hierarchyLevel, broaderCodeId, language, statusList, meta);
            if (FORMAT_CSV.equalsIgnoreCase(format)) {
                final String csv = codeExporter.createCsv(codes);
                return streamCsvCodesOutput(csv);
            } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
                final Workbook workbook = codeExporter.createExcel(codes, format);
                return streamExcelCodesOutput(workbook);
            } else if (array != null) {
                final ObjectMapper mapper = new ObjectMapper();
                final ArrayNode arrayNode = mapper.createArrayNode();
                codes.forEach(code -> arrayNode.add(code.getCodeValue()));
                Response response = Response.ok(arrayNode).build();
                if (downloadArray) {
                    response.getHeaders().putSingle(HEADER_CONTENT_DISPOSITION, "attachment; filename = " + "codelist_" + codeScheme.getCodeValue() + "_codes.json");
                }
                return response;
            } else {
                ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODE, expand), pretty));
                if (pageSize != null && from + pageSize < meta.getTotalResults()) {
                    meta.setNextPage(apiUtils.createNextPageUrl(API_VERSION, API_PATH_CODEREGISTRIES + "/" + codeRegistryCodeValue + API_PATH_CODESCHEMES + "/" + codeSchemeCodeValue + API_PATH_CODES, after, pageSize, from + pageSize));
                }
                final ResponseWrapper<CodeDTO> wrapper = new ResponseWrapper<>();
                wrapper.setMeta(meta);
                if (codes == null) {
                    throw new NotFoundException();
                }
                wrapper.setResults(codes);
                return Response.ok(wrapper).build();
            }
        } else {
            throw new NotFoundException();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/extensions/")
    @Operation(description = "Return Extensions for a CodeScheme.")
    @ApiResponse(responseCode = "200", description = "Returns all Extensions for CodeScheme.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8" })
    @Tag(name = "Extension")
    public Response getCodeRegistryCodeSchemeExtensions(@Parameter(description = "Pagination parameter for page size.", in = ParameterIn.QUERY) @QueryParam("pageSize") final Integer pageSize,
                                                        @Parameter(description = "Pagination parameter for start index.", in = ParameterIn.QUERY) @QueryParam("from") @DefaultValue("0") final Integer from,
                                                        @Parameter(description = "CodeRegistry CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                        @Parameter(description = "CodeScheme CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                        @Parameter(description = "Extension PrefLabel.", in = ParameterIn.QUERY) @QueryParam("prefLabel") final String prefLabel,
                                                        @Parameter(description = "Format for content.", in = ParameterIn.QUERY) @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                                        @Parameter(description = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("after") final String after,
                                                        @Parameter(description = "Before date filtering parameter, results will be codes with modified date before this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("before") final String before,
                                                        @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                                        @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        final Meta meta = new Meta(200, pageSize, from, parseDateFromString(after), parseDateFromString(before));
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);
        if (codeScheme != null) {
            final Set<ExtensionDTO> extensions = domain.getExtensions(codeScheme, prefLabel, meta);
            if (FORMAT_CSV.startsWith(format.toLowerCase())) {
                final String csv = extensionExporter.createCsv(extensions);
                return streamCsvExtensionsOutput(csv);
            } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
                final Workbook workbook = extensionExporter.createExcel(extensions, format);
                return streamExcelExtensionsOutput(workbook);
            } else {
                ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_EXTENSION, expand), pretty));
                if (pageSize != null && from + pageSize < meta.getTotalResults()) {
                    meta.setNextPage(apiUtils.createNextPageUrl(API_VERSION, API_PATH_CODEREGISTRIES + "/" + codeRegistryCodeValue + API_PATH_CODESCHEMES + "/" + codeSchemeCodeValue + API_PATH_EXTENSIONS, after, pageSize, from + pageSize));
                }
                final ResponseWrapper<ExtensionDTO> wrapper = new ResponseWrapper<>();
                wrapper.setMeta(meta);
                if (extensions == null) {
                    throw new NotFoundException();
                }
                wrapper.setResults(extensions);
                return Response.ok(wrapper).build();
            }
        } else {
            throw new NotFoundException();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/extensions/{extensionCodeValue}")
    @Operation(description = "Return Extension for a CodeScheme.")
    @ApiResponse(responseCode = "200", description = "Returns single Extension for CodeScheme.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8" })
    @Tag(name = "Extension")
    public Response getCodeRegistryCodeSchemeExtension(@Parameter(description = "CodeRegistry CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                       @Parameter(description = "CodeScheme CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                       @Parameter(description = "Extension CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("extensionCodeValue") final String extensionCodeValue,
                                                       @Parameter(description = "Format for content.", in = ParameterIn.QUERY) @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                                       @Parameter(description = "Is this a Cross-Refence List or not.", in = ParameterIn.QUERY) @QueryParam("crossreferencelist") @DefaultValue("false") final boolean exportAsSimplifiedCrossReferenceList,
                                                       @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                                       @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        final ExtensionDTO extension = domain.getExtension(codeRegistryCodeValue, codeSchemeCodeValue, extensionCodeValue);
        if (extension != null) {
            if (FORMAT_CSV.startsWith(format.toLowerCase())) {
                final Set<ExtensionDTO> extensions = new HashSet<>();
                extensions.add(extension);
                final String csv = extensionExporter.createCsv(extensions);
                return streamCsvExtensionsOutput(csv);
            } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
                final Workbook workbook = extensionExporter.createExcel(extension, format, exportAsSimplifiedCrossReferenceList);
                if (exportAsSimplifiedCrossReferenceList) {
                    return streamExcelCrossReferenceListOutput(workbook);
                } else {
                    return streamExcelExtensionsOutput(workbook);
                }
            } else {
                ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_EXTENSION, expand), pretty));
                return Response.ok(extension).build();
            }
        } else {
            throw new NotFoundException();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/extensions/{extensionCodeValue}/members/")
    @Operation(description = "Return Members for an Extension.")
    @ApiResponse(responseCode = "200", description = "Returns all Members for an Extension.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "text/csv" })
    @Tag(name = "Member")
    public Response getCodeRegistryCodeSchemeExtensionMembers(@Parameter(description = "Pagination parameter for page size.", in = ParameterIn.QUERY) @QueryParam("pageSize") final Integer pageSize,
                                                              @Parameter(description = "Pagination parameter for start index.", in = ParameterIn.QUERY) @QueryParam("from") @DefaultValue("0") final Integer from,
                                                              @Parameter(description = "CodeRegistry CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                              @Parameter(description = "CodeScheme CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                              @Parameter(description = "Extension CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("extensionCodeValue") final String extensionCodeValue,
                                                              @Parameter(description = "Extension PrefLabel.", in = ParameterIn.QUERY) @QueryParam("prefLabel") final String prefLabel,
                                                              @Parameter(description = "Format for content.", in = ParameterIn.QUERY) @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                                              @Parameter(description = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("after") final String after,
                                                              @Parameter(description = "Before date filtering parameter, results will be codes with modified date before this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("before") final String before,
                                                              @Parameter(description = "Is this a Cross-Refence List or not.", in = ParameterIn.QUERY) @QueryParam("crossreferencelist") @DefaultValue("false") final boolean exportAsSimplifiedCrossReferenceList,
                                                              @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                                              @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        final Meta meta = new Meta(200, pageSize, from, parseDateFromString(after), parseDateFromString(before));
        final ExtensionDTO extension = domain.getExtension(codeRegistryCodeValue, codeSchemeCodeValue, extensionCodeValue);
        if (extension != null) {
            final Set<MemberDTO> members = domain.getMembers(extension, meta);
            if (FORMAT_CSV.startsWith(format.toLowerCase())) {
                if (exportAsSimplifiedCrossReferenceList) {
                    return streamCsvCrossReferenceListOutput(memberExporter.createSimplifiedCsvForCrossReferenceList(extension, members));
                } else {
                    return streamCsvMembersOutput(memberExporter.createCsv(extension, members));
                }
            } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
                final Workbook workbook = memberExporter.createExcel(extension, members, format);
                return streamExcelMembersOutput(workbook);
            } else {
                ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_MEMBER, expand), pretty));
                if (pageSize != null && from + pageSize < meta.getTotalResults()) {
                    meta.setNextPage(apiUtils.createNextPageUrl(API_VERSION, API_PATH_CODEREGISTRIES + "/" + codeRegistryCodeValue + API_PATH_CODESCHEMES + "/" + codeSchemeCodeValue + API_PATH_EXTENSIONS + "/" + extensionCodeValue + API_PATH_MEMBERS, after, pageSize, from + pageSize));
                }
                final ResponseWrapper<MemberDTO> wrapper = new ResponseWrapper<>();
                wrapper.setMeta(meta);
                if (members == null) {
                    throw new NotFoundException();
                }
                wrapper.setResults(members);
                return Response.ok(wrapper).build();
            }
        } else {
            throw new NotFoundException();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/extensions/{extensionCodeValue}/members/{memberId}")
    @Operation(description = "Return Member for an Extension.")
    @ApiResponse(responseCode = "200", description = "Returns single Member for Extension.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8" })
    @Tag(name = "Member")
    public Response getCodeRegistryCodeSchemeExtensionMember(@Parameter(description = "CodeRegistry CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                             @Parameter(description = "CodeScheme CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                             @Parameter(description = "Extension CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("extensionCodeValue") final String extensionCodeValue,
                                                             @Parameter(description = "Member ID.", in = ParameterIn.PATH, required = true) @PathParam("memberId") final String memberId,
                                                             @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                                             @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        final ExtensionDTO extension = domain.getExtension(codeRegistryCodeValue, codeSchemeCodeValue, extensionCodeValue);
        if (extension != null) {
            final MemberDTO member = domain.getMember(memberId, extensionCodeValue, codeSchemeCodeValue);
            if (member != null) {
                ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_MEMBER, expand), pretty));
                return Response.ok(member).build();
            } else {
                throw new NotFoundException();
            }
        } else {
            throw new NotFoundException();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/externalreferences/")
    @Operation(description = "Return ExternalReferences for a CodeScheme.")
    @ApiResponse(responseCode = "200", description = "Returns all ExternalReferences for CodeScheme.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "text/csv" })
    @Tag(name = "ExternalReference")
    public Response getCodeRegistryCodeSchemeExternalReferences(@Parameter(description = "Pagination parameter for page size.", in = ParameterIn.QUERY) @QueryParam("pageSize") final Integer pageSize,
                                                                @Parameter(description = "Pagination parameter for start index.", in = ParameterIn.QUERY) @QueryParam("from") @DefaultValue("0") final Integer from,
                                                                @Parameter(description = "CodeRegistry CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                                @Parameter(description = "CodeScheme CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                                @Parameter(description = "Extension PrefLabel.", in = ParameterIn.QUERY) @QueryParam("prefLabel") final String prefLabel,
                                                                @Parameter(description = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("after") final String after,
                                                                @Parameter(description = "Before date filtering parameter, results will be codes with modified date before this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("before") final String before,
                                                                @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                                                @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        final Meta meta = new Meta(200, pageSize, from, parseDateFromString(after), parseDateFromString(before));
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);
        if (codeScheme != null) {
            ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_EXTERNALREFERENCE, expand), pretty));
            final Set<ExternalReferenceDTO> externalReferences = domain.getExternalReferences(prefLabel, codeScheme, false, meta);
            if (pageSize != null && from + pageSize < meta.getTotalResults()) {
                meta.setNextPage(apiUtils.createNextPageUrl(API_VERSION, API_PATH_CODEREGISTRIES + "/" + codeRegistryCodeValue + API_PATH_CODESCHEMES + "/" + codeSchemeCodeValue + API_PATH_EXTERNALREFERENCES, after, pageSize, from + pageSize));
            }
            final ResponseWrapper<ExternalReferenceDTO> wrapper = new ResponseWrapper<>();
            wrapper.setMeta(meta);
            if (externalReferences == null) {
                throw new NotFoundException();
            }
            wrapper.setResults(externalReferences);
            return Response.ok(wrapper).build();
        } else {
            throw new NotFoundException();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/codes/{codeCodeValue}")
    @Operation(description = "Return one Code from specific CodeScheme under specific CodeRegistry.")
    @ApiResponse(responseCode = "200", description = "Returns one Code from specific CodeRegistry in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    @Tag(name = "CodeScheme")
    public Response getCodeRegistryCodeSchemeCode(@Parameter(description = "CodeRegistry CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                  @Parameter(description = "CodeScheme CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                  @Parameter(description = "Code code.", in = ParameterIn.PATH, required = true) @Encoded @PathParam("codeCodeValue") final String codeCodeValue,
                                                  @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                                  @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODE, expand), pretty));
        final CodeDTO code = domain.getCode(codeRegistryCodeValue, codeSchemeCodeValue, urlDecodeCodeValue(codeCodeValue));
        if (code != null) {
            return Response.ok(code).build();
        }
        throw new NotFoundException();
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/codes/{codeCodeValue}/members/")
    @Operation(description = "Return Members for a Code.")
    @ApiResponse(responseCode = "200", description = "Returns all Members for Code.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8" })
    @Tag(name = "Member")
    public Response getCodeRegistryCodeSchemeCodeMembers(@Parameter(description = "Pagination parameter for page size.", in = ParameterIn.QUERY) @QueryParam("pageSize") final Integer pageSize,
                                                         @Parameter(description = "Pagination parameter for start index.", in = ParameterIn.QUERY) @QueryParam("from") @DefaultValue("0") final Integer from,
                                                         @Parameter(description = "CodeRegistry CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                         @Parameter(description = "CodeScheme CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                         @Parameter(description = "Extension PrefLabel.", in = ParameterIn.QUERY) @QueryParam("prefLabel") final String prefLabel,
                                                         @Parameter(description = "Code code.", in = ParameterIn.PATH, required = true) @Encoded @PathParam("codeCodeValue") final String codeCodeValue,
                                                         @Parameter(description = "Format for content.", in = ParameterIn.QUERY) @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                                         @Parameter(description = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("after") final String after,
                                                         @Parameter(description = "Before date filtering parameter, results will be codes with modified date before this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("before") final String before,
                                                         @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                                         @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        final Meta meta = new Meta(200, pageSize, from, parseDateFromString(after), parseDateFromString(before));
        final CodeDTO code = domain.getCode(codeRegistryCodeValue, codeSchemeCodeValue, urlDecodeCodeValue(codeCodeValue));
        if (code != null) {
            final Set<MemberDTO> members = domain.getMembers(code, meta);
            if (FORMAT_CSV.startsWith(format.toLowerCase())) {
                final String csv = memberExporter.createCsv(null, members);
                return streamCsvMembersOutput(csv);
            } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
                final Workbook workbook = memberExporter.createExcel(null, members, format);
                return streamExcelMembersOutput(workbook);
            } else {
                ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_MEMBER, expand), pretty));
                if (pageSize != null && from + pageSize < meta.getTotalResults()) {
                    meta.setNextPage(apiUtils.createNextPageUrl(API_VERSION, API_PATH_CODEREGISTRIES + "/" + codeRegistryCodeValue + API_PATH_CODESCHEMES + "/" + codeSchemeCodeValue + API_PATH_CODES + "/" + codeCodeValue + API_PATH_MEMBERS, after, pageSize, from + pageSize));
                }
                final ResponseWrapper<MemberDTO> wrapper = new ResponseWrapper<>();
                wrapper.setMeta(meta);
                if (members == null) {
                    throw new NotFoundException();
                }
                wrapper.setResults(members);
                return Response.ok(wrapper).build();
            }
        } else {
            throw new NotFoundException();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/versions/")
    @Operation(description = "Return the complete version history of a specific CodeScheme, latest first.")
    @ApiResponse(responseCode = "200", description = "Return the complete version history of a specific CodeScheme, latest first, in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    @Tag(name = "CodeScheme")
    public Response getCodeSchemeVersions(@Parameter(description = "CodeRegistry codevalue.", in = ParameterIn.PATH, required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                          @Parameter(description = "CodeScheme codevalue.", in = ParameterIn.PATH, required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                          @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                          @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODESCHEME, expand), pretty));
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);
        if (codeScheme == null) {
            throw new NotFoundException();
        }
        final LinkedHashSet<CodeSchemeListItem> allVersions = codeScheme.getAllVersions();
        final LinkedHashSet<CodeSchemeDTO> results = new LinkedHashSet<>();
        if (allVersions == null || allVersions.isEmpty()) {
            results.add(codeScheme);
        } else {
            allVersions.forEach(version -> results.add(domain.getCodeScheme(version.getId().toString())));
        }
        final Meta meta = new Meta(200, null, null, null, null);
        final ResponseWrapper<CodeSchemeDTO> wrapper = new ResponseWrapper<>();
        meta.setResultCount(results.size());
        wrapper.setResults(results);
        wrapper.setMeta(meta);
        return Response.ok(wrapper).build();
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/variants/")
    @Operation(description = "Returns the variants of the CodeScheme.")
    @ApiResponse(responseCode = "200", description = "Return the variants of the CodeScheme in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    @Tag(name = "CodeScheme")
    public Response getCodeSchemeVariants(@Parameter(description = "CodeRegistry codevalue.", in = ParameterIn.PATH, required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                          @Parameter(description = "CodeScheme codevalue.", in = ParameterIn.PATH, required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                          @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                          @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODESCHEME, expand), pretty));
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);
        if (codeScheme == null) {
            throw new NotFoundException();
        }
        final LinkedHashSet<CodeSchemeDTO> result = new LinkedHashSet<>();
        final LinkedHashSet<CodeSchemeListItem> variants = codeScheme.getVariantsOfThisCodeScheme();
        if (variants != null && !variants.isEmpty()) {
            variants.forEach(variant -> result.add(domain.getCodeScheme(variant.getId().toString())));
        }
        final Meta meta = new Meta(200, null, null, null, null);
        meta.setResultCount(result.size());
        final ResponseWrapper<CodeSchemeDTO> wrapper = new ResponseWrapper<>();
        wrapper.setResults(result);
        wrapper.setMeta(meta);
        return Response.ok(wrapper).build();
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/variantmothers/")
    @Operation(description = "Returns the CodeSchemes of which this CodeScheme is a variant of.")
    @ApiResponse(responseCode = "200", description = "Return the CodeSchemes of which this CodeScheme is a variant of in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    @Tag(name = "CodeScheme")
    public Response getCodeSchemeVariantMothers(@Parameter(description = "CodeRegistry codevalue.", in = ParameterIn.PATH, required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                @Parameter(description = "CodeScheme codevalue.", in = ParameterIn.PATH, required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                                @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODESCHEME, expand), pretty));
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);
        if (codeScheme == null) {
            throw new NotFoundException();
        }
        final LinkedHashSet<CodeSchemeListItem> variantMothers = codeScheme.getVariantMothersOfThisCodeScheme();
        final LinkedHashSet<CodeSchemeDTO> result = new LinkedHashSet<>();

        if (variantMothers != null && !variantMothers.isEmpty()) {
            variantMothers.forEach(variantMother -> result.add(domain.getCodeScheme(variantMother.getId().toString())));
        }
        final Meta meta = new Meta(200, null, null, null, null);
        meta.setResultCount(result.size());
        final ResponseWrapper<CodeSchemeDTO> wrapper = new ResponseWrapper<>();
        wrapper.setResults(result);
        wrapper.setMeta(meta);
        return Response.ok(wrapper).build();
    }

    private void filterCodes(final Set<CodeDTO> codes) {
        codes.forEach(code -> code.setCodeScheme(null));
    }

    private void filterExtensions(final Set<ExtensionDTO> extensions) {
        extensions.forEach(extension -> extension.setParentCodeScheme(null));
    }

    private void filterMembers(final Set<MemberDTO> members) {
        members.forEach(member -> {
            member.getCode().setCodeScheme(null);
            member.setExtension(null);
        });
    }
}
