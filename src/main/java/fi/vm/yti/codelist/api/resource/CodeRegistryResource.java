package fi.vm.yti.codelist.api.resource;

import java.util.HashSet;
import java.util.LinkedHashSet;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterInjector;

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
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;
import static java.util.Arrays.asList;

@Component
@Path("/v1/coderegistries")
@Api(value = "coderegistries")
@Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
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
    @ApiOperation(value = "Return a list of available CodeRegistries.", response = CodeRegistryDTO.class, responseContainer = "List")
    @ApiResponse(code = 200, message = "Returns all CodeRegistries in specified format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
    public Response getCodeRegistries(@ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                      @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                      @ApiParam(value = "CodeRegistry CodeValue as string value.") @QueryParam("codeValue") final String codeRegistryCodeValue,
                                      @ApiParam(value = "CodeRegistry name as string value.") @QueryParam("name") final String name,
                                      @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                      @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                      @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                      @ApiParam(value = "Organizations filtering parameter, results will be registries belonging to these organizations") @QueryParam("organizations") final String organizationsCsv,
                                      @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
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
            final Meta meta = new Meta(200, pageSize, from, after);
            ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODEREGISTRY, expand), pretty));
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
                                    @ApiParam(value = "Language code for sorting results.") @QueryParam("language") @DefaultValue("fi") final String language,
                                    @ApiParam(value = "Boolean that controls whether to embed CodeSchemes in payload or not.") @QueryParam("embedCodeSchemes") @DefaultValue("false") final Boolean embedCodeSchemes,
                                    @ApiParam(value = "User organizations filtering parameter, for filtering unfinished code schemes") @QueryParam("userOrganizations") final String userOrganizationsCsv,
                                    @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODEREGISTRY, expand), pretty));
        final List<String> userOrganizations = userOrganizationsCsv == null ? null : asList(userOrganizationsCsv.toLowerCase().split(","));
        final CodeRegistryDTO codeRegistry = domain.getCodeRegistry(codeRegistryCodeValue);
        if (codeRegistry != null) {
            if (embedCodeSchemes) {
                codeRegistry.setCodeSchemes(domain.getCodeSchemesByCodeRegistryCodeValue(codeRegistryCodeValue, null, userOrganizations, language));
            }
            return Response.ok(codeRegistry).build();
        } else {
            throw new NotFoundException();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/")
    @ApiOperation(value = "Return CodeSchemes for a CodeRegistry.", response = CodeRegistryDTO.class, responseContainer = "List")
    @ApiResponse(code = 200, message = "Returns CodeSchemes for a CodeRegistry in specified format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
    public Response getCodeRegistryCodeSchemes(@ApiParam(value = "CodeRegistry CodeValue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                               @ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                               @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                               @ApiParam(value = "Service information domain classifications in CSL format.") @QueryParam("infoDomain") final String infoDomain,
                                               @ApiParam(value = "CodeRegistry PrefLabel as string value for searching.") @QueryParam("codeRegistryPrefLabel") final String codeRegistryPrefLabel,
                                               @ApiParam(value = "CodeScheme codeValue as string value for searching.") @QueryParam("codeValue") final String codeSchemeCodeValue,
                                               @ApiParam(value = "CodeScheme PrefLabel as string value for searching.") @QueryParam("prefLabel") final String codeSchemePrefLabel,
                                               @ApiParam(value = "Language code for sorting results.") @QueryParam("language") @DefaultValue("fi") final String language,
                                               @ApiParam(value = "Search term for matching codeValue and prefLabel.") @QueryParam("searchTerm") final String searchTerm,
                                               @ApiParam(value = "Status enumerations in CSL format.") @QueryParam("status") final String status,
                                               @ApiParam(value = "Extension PropertyType localName as string value for searching.") @QueryParam("extensionPropertyType") final String extensionPropertyType,
                                               @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                               @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                               @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                               @ApiParam(value = "Sort mode for response values.") @QueryParam("sortMode") @DefaultValue("default") final String sortMode,
                                               @ApiParam(value = "User organizations filtering parameter, for filtering unfinished code schemes") @QueryParam("userOrganizations") final String userOrganizationsCsv,
                                               @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
        final Meta meta = new Meta(200, pageSize, from, after);
        final List<String> userOrganizations = userOrganizationsCsv == null ? null : asList(userOrganizationsCsv.toLowerCase().split(","));
        final List<String> infoDomainsList = parseInfoDomains(infoDomain);
        final List<String> statusList = parseStatus(status);
        final CodeRegistryDTO codeRegistry = domain.getCodeRegistry(codeRegistryCodeValue);
        if (codeRegistry != null) {
            if (FORMAT_CSV.equalsIgnoreCase(format.toLowerCase())) {
                final Set<CodeSchemeDTO> codeSchemes = domain.getCodeSchemes(pageSize, from, sortMode, null, userOrganizations, codeRegistryCodeValue, codeRegistryPrefLabel, codeSchemeCodeValue, codeSchemePrefLabel, language, searchTerm, false, false, statusList, infoDomainsList, extensionPropertyType, Meta.parseAfterFromString(after), null);
                final String csv = codeSchemeExporter.createCsv(codeSchemes);
                return streamCsvCodeSchemesOutput(csv);
            } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
                final Set<CodeSchemeDTO> codeSchemes = domain.getCodeSchemes(pageSize, from, sortMode, null, userOrganizations, codeRegistryCodeValue, codeRegistryPrefLabel, codeSchemeCodeValue, codeSchemePrefLabel, language, searchTerm, false, false, statusList, infoDomainsList, extensionPropertyType, Meta.parseAfterFromString(after), null);
                final Workbook workbook = codeSchemeExporter.createExcel(codeSchemes, format);
                return streamExcelCodeSchemesOutput(workbook);
            } else {
                ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODESCHEME, expand), pretty));
                final Set<CodeSchemeDTO> codeSchemes = domain.getCodeSchemes(pageSize, from, sortMode, null, userOrganizations, codeRegistryCodeValue, codeRegistryPrefLabel, codeSchemeCodeValue, codeSchemePrefLabel, language, searchTerm, false, false, statusList, infoDomainsList, extensionPropertyType, meta.getAfter(), meta);
                meta.setResultCount(codeSchemes.size());
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
    @ApiOperation(value = "Return one specific CodeScheme.", response = CodeSchemeDTO.class)
    @ApiResponse(code = 200, message = "Returns one specific CodeScheme in JSON format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", MediaType.TEXT_PLAIN + ";charset=UTF-8", "application/xlsx", "application/csv" })
    public Response getCodeRegistryCodeScheme(@ApiParam(value = "CodeRegistry CodeValue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                              @ApiParam(value = "CodeScheme CodeValue.", required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                              @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                              @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                              @ApiParam(value = "Download JSON as a file.") @QueryParam("downloadFile") @DefaultValue("false") final boolean downloadFile,
                                              @ApiParam(value = "Boolean that controls whether to embed Codes in the payload or not.") @QueryParam("embedCodes") @DefaultValue("false") final Boolean embedCodes,
                                              @ApiParam(value = "Boolean that controls whether to embed Extensions in the payload or not.") @QueryParam("embedExtensions") @DefaultValue("false") final Boolean embedExtensions,
                                              @ApiParam(value = "Boolean that controls whether to embed embedMembers in the payload or not.") @QueryParam("embedMembers") @DefaultValue("false") final Boolean embedMembers,
                                              @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODESCHEME, expand), pretty));
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
                        Set<ExtensionDTO> extensions = domain.getExtensions(null, null, null, codeScheme, null, null);
                        filterExtensions(extensions);
                        if (embedMembers) {
                            for (ExtensionDTO extension : extensions) {
                                final Set<MemberDTO> members = domain.getMembers(null, null, extension, null, null);
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
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/codes/")
    @ApiOperation(value = "Return Codes for a CodeScheme.", response = CodeDTO.class)
    @ApiResponse(code = 200, message = "Returns all Codes for CodeScheme in specified format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
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
                                                   @ApiParam(value = "Language code for sorting results.") @QueryParam("language") final String language,
                                                   @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                                   @ApiParam(value = "Returns code codeValues in JSON array format") @QueryParam("array") final String array,
                                                   @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
        final Meta meta = new Meta(Response.Status.OK.getStatusCode(), pageSize, from, after);
        final List<String> statusList = parseStatus(status);
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);
        if (codeScheme != null) {
            if (FORMAT_CSV.equalsIgnoreCase(format)) {
                final Set<CodeDTO> codes = domain.getCodes(pageSize, from, codeRegistryCodeValue, codeSchemeCodeValue, codeCodeValue, prefLabel, hierarchyLevel, broaderCodeId, language, statusList, Meta.parseAfterFromString(after), null);
                final String csv = codeExporter.createCsv(codes);
                return streamCsvCodesOutput(csv);
            } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
                final Set<CodeDTO> codes = domain.getCodes(pageSize, from, codeRegistryCodeValue, codeSchemeCodeValue, codeCodeValue, prefLabel, hierarchyLevel, broaderCodeId, language, statusList, Meta.parseAfterFromString(after), null);
                final Workbook workbook = codeExporter.createExcel(codes, format);
                return streamExcelCodesOutput(workbook);
            } else if (array != null) {
                final Set<CodeDTO> codes = domain.getCodes(pageSize, from, codeRegistryCodeValue, codeSchemeCodeValue, codeCodeValue, prefLabel, hierarchyLevel, broaderCodeId, language, statusList, meta.getAfter(), meta);
                final ObjectMapper mapper = new ObjectMapper();
                final ArrayNode arrayNode = mapper.createArrayNode();
                codes.stream().forEach(code -> arrayNode.add(code.getCodeValue()));
                return Response.ok(arrayNode).build();
            } else {
                ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODE, expand), pretty));
                final Set<CodeDTO> codes = domain.getCodes(pageSize, from, codeRegistryCodeValue, codeSchemeCodeValue, codeCodeValue, prefLabel, hierarchyLevel, broaderCodeId, language, statusList, meta.getAfter(), meta);
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
    @ApiOperation(value = "Return Extensions for a CodeScheme.", response = ExtensionDTO.class)
    @ApiResponse(code = 200, message = "Returns all Extensions for CodeScheme.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8" })
    public Response getCodeRegistryCodeSchemeExtensions(@ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                                        @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                                        @ApiParam(value = "CodeRegistry CodeValue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                        @ApiParam(value = "CodeScheme CodeValue.", required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                        @ApiParam(value = "Extension PrefLabel.") @QueryParam("prefLabel") final String prefLabel,
                                                        @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                                        @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                                        @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                                        @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
        final Meta meta = new Meta(Response.Status.OK.getStatusCode(), pageSize, from, after);
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);
        if (codeScheme != null) {
            if (FORMAT_CSV.startsWith(format.toLowerCase())) {
                final Set<ExtensionDTO> extensions = domain.getExtensions(pageSize, from, prefLabel, codeScheme, Meta.parseAfterFromString(after), null);
                final String csv = extensionExporter.createCsv(extensions);
                return streamCsvExtensionsOutput(csv);
            } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
                final Set<ExtensionDTO> extensions = domain.getExtensions(pageSize, from, prefLabel, codeScheme, Meta.parseAfterFromString(after), null);
                final Workbook workbook = extensionExporter.createExcel(extensions, format);
                return streamExcelExtensionsOutput(workbook);
            } else {
                ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_EXTENSION, expand), pretty));
                final Set<ExtensionDTO> extensions = domain.getExtensions(pageSize, from, prefLabel, codeScheme, meta.getAfter(), meta);
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
    @ApiOperation(value = "Return Extension for a CodeScheme.", response = ExtensionDTO.class)
    @ApiResponse(code = 200, message = "Returns single Extension for CodeScheme.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8" })
    public Response getCodeRegistryCodeSchemeExtension(@ApiParam(value = "CodeRegistry CodeValue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                       @ApiParam(value = "CodeScheme CodeValue.", required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                       @ApiParam(value = "Extension CodeValue.", required = true) @PathParam("extensionCodeValue") final String extensionCodeValue,
                                                       @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                                       @ApiParam(value = "Is this a Cross-Refence List or not.") @QueryParam("crossreferencelist") @DefaultValue("false") final boolean exportAsSimplifiedCrossReferenceList,
                                                       @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                                       @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
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
                ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_EXTENSION, expand), pretty));
                return Response.ok(extension).build();
            }
        } else {
            throw new NotFoundException();
        }
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/extensions/{extensionCodeValue}/members/")
    @ApiOperation(value = "Return Members for an Extension.", response = MemberDTO.class)
    @ApiResponse(code = 200, message = "Returns all Members for an Extension.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
    public Response getCodeRegistryCodeSchemeExtensionMembers(@ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                                              @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                                              @ApiParam(value = "CodeRegistry CodeValue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                              @ApiParam(value = "CodeScheme CodeValue.", required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                              @ApiParam(value = "Extension CodeValue.", required = true) @PathParam("extensionCodeValue") final String extensionCodeValue,
                                                              @ApiParam(value = "Extension PrefLabel.") @QueryParam("prefLabel") final String prefLabel,
                                                              @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                                              @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                                              @ApiParam(value = "Is this a Cross-Refence List or not.") @QueryParam("crossreferencelist") @DefaultValue("false") final boolean exportAsSimplifiedCrossReferenceList,
                                                              @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                                              @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {

        final Meta meta = new Meta(Response.Status.OK.getStatusCode(), pageSize, from, after);
        final ExtensionDTO extension = domain.getExtension(codeRegistryCodeValue, codeSchemeCodeValue, extensionCodeValue);
        if (extension != null) {
            if (FORMAT_CSV.startsWith(format.toLowerCase())) {
                final Set<MemberDTO> members = domain.getMembers(pageSize, from, extension, meta.getAfter(), meta);
                if (exportAsSimplifiedCrossReferenceList) {
                    return streamCsvCrossReferenceListOutput(memberExporter.createSimplifiedCsvForCrossReferenceList(extension, members));
                } else {
                    return streamCsvMembersOutput(memberExporter.createCsv(extension, members));
                }
            } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
                final Set<MemberDTO> members = domain.getMembers(pageSize, from, extension, meta.getAfter(), meta);
                final Workbook workbook = memberExporter.createExcel(extension, members, format);
                return streamExcelMembersOutput(workbook);
            } else {
                ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_MEMBER, expand), pretty));
                final Set<MemberDTO> members = domain.getMembers(pageSize, from, extension, meta.getAfter(), meta);
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
    @ApiOperation(value = "Return Member for an Extension.", response = ExtensionDTO.class)
    @ApiResponse(code = 200, message = "Returns single Member for Extension.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8" })
    public Response getCodeRegistryCodeSchemeExtensionMember(@ApiParam(value = "CodeRegistry CodeValue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                             @ApiParam(value = "CodeScheme CodeValue.", required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                             @ApiParam(value = "Extension CodeValue.", required = true) @PathParam("extensionCodeValue") final String extensionCodeValue,
                                                             @ApiParam(value = "Member ID.", required = true) @PathParam("memberId") final String memberId,
                                                             @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                                             @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
        final ExtensionDTO extension = domain.getExtension(codeRegistryCodeValue, codeSchemeCodeValue, extensionCodeValue);
        if (extension != null) {
            final MemberDTO member = domain.getMember(memberId, extensionCodeValue);
            if (member != null) {
                ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_MEMBER, expand), pretty));
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
    @ApiOperation(value = "Return ExternalReferences for a CodeScheme.", response = ExternalReferenceDTO.class)
    @ApiResponse(code = 200, message = "Returns all ExternalReferences for CodeScheme.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
    public Response getCodeRegistryCodeSchemeExternalReferences(@ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                                                @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                                                @ApiParam(value = "CodeRegistry CodeValue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                                @ApiParam(value = "CodeScheme CodeValue.", required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                                @ApiParam(value = "Extension PrefLabel.") @QueryParam("prefLabel") final String prefLabel,
                                                                @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                                                @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                                                @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
        final Meta meta = new Meta(Response.Status.OK.getStatusCode(), pageSize, from, after);
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);
        if (codeScheme != null) {
            ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODE, expand), pretty));
            final Set<ExternalReferenceDTO> externalReferences = domain.getExternalReferences(pageSize, from, prefLabel, codeScheme, false, meta.getAfter(), meta);
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
    @ApiOperation(value = "Return one Code from specific CodeScheme under specific CodeRegistry.", response = CodeDTO.class)
    @ApiResponse(code = 200, message = "Returns one Code from specific CodeRegistry in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getCodeRegistryCodeSchemeCode(@ApiParam(value = "CodeRegistry CodeValue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                  @ApiParam(value = "CodeScheme CodeValue.", required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                  @ApiParam(value = "Code code.", required = true) @PathParam("codeCodeValue") final String codeCodeValue,
                                                  @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                                  @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODE, expand), pretty));
        final CodeDTO code = domain.getCode(codeRegistryCodeValue, codeSchemeCodeValue, codeCodeValue);
        if (code != null) {
            return Response.ok(code).build();
        }
        throw new NotFoundException();
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/codes/{codeCodeValue}/members/")
    @ApiOperation(value = "Return Members for a Code.", response = MemberDTO.class)
    @ApiResponse(code = 200, message = "Returns all Members for Code.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8" })
    public Response getCodeRegistryCodeSchemeCodeMembers(@ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                                         @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                                         @ApiParam(value = "CodeRegistry CodeValue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                         @ApiParam(value = "CodeScheme CodeValue.", required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                         @ApiParam(value = "Extension PrefLabel.") @QueryParam("prefLabel") final String prefLabel,
                                                         @ApiParam(value = "Code code.", required = true) @PathParam("codeCodeValue") final String codeCodeValue,
                                                         @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                                         @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                                         @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                                         @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
        final Meta meta = new Meta(Response.Status.OK.getStatusCode(), pageSize, from, after);
        final CodeDTO code = domain.getCode(codeRegistryCodeValue, codeSchemeCodeValue, urlDecodeString(codeCodeValue));
        if (code != null) {
            if (FORMAT_CSV.startsWith(format.toLowerCase())) {
                final Set<MemberDTO> members = domain.getMembers(pageSize, from, code, meta.getAfter(), meta);
                final String csv = memberExporter.createCsv(null, members);
                return streamCsvMembersOutput(csv);
            } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
                final Set<MemberDTO> members = domain.getMembers(pageSize, from, code, meta.getAfter(), meta);
                final Workbook workbook = memberExporter.createExcel(null, members, format);
                return streamExcelMembersOutput(workbook);
            } else {
                ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_MEMBER, expand), pretty));
                final Set<MemberDTO> members = domain.getMembers(pageSize, from, code, meta.getAfter(), meta);
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
    @ApiOperation(value = "Return the complete version history of a specific CodeScheme, latest first.", response = CodeSchemeDTO.class, responseContainer = "List")
    @ApiResponse(code = 200, message = "Return the complete version history of a specific CodeScheme, latest first, in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getCodeSchemeVersions(@ApiParam(value = "CodeRegistry codevalue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                          @ApiParam(value = "CodeScheme codevalue.", required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                          @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                          @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODESCHEME, expand), pretty));
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

        final Meta meta = new Meta(200, null, null, null);
        final ResponseWrapper<CodeSchemeDTO> wrapper = new ResponseWrapper<>();
        meta.setResultCount(results.size());
        wrapper.setResults(results);
        wrapper.setMeta(meta);
        return Response.ok(wrapper).build();
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/variants/")
    @ApiOperation(value = "Returns the variants of the CodeScheme.", response = CodeSchemeDTO.class, responseContainer = "List")
    @ApiResponse(code = 200, message = "Return the variants of the CodeScheme in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getCodeSchemeVariants(@ApiParam(value = "CodeRegistry codevalue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                          @ApiParam(value = "CodeScheme codevalue.", required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                          @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                          @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODESCHEME, expand), pretty));
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);

        if (codeScheme == null) {
            throw new NotFoundException();
        }

        final LinkedHashSet<CodeSchemeDTO> result = new LinkedHashSet<>();
        final LinkedHashSet<CodeSchemeListItem> variants = codeScheme.getVariantsOfThisCodeScheme();

        if (variants != null && !variants.isEmpty()) {
            variants.forEach(variant -> result.add(domain.getCodeScheme(variant.getId().toString())));
        }

        final Meta meta = new Meta(200, null, null, null);
        meta.setResultCount(result.size());
        final ResponseWrapper<CodeSchemeDTO> wrapper = new ResponseWrapper<>();
        wrapper.setResults(result);
        wrapper.setMeta(meta);
        return Response.ok(wrapper).build();
    }

    @GET
    @Path("{codeRegistryCodeValue}/codeschemes/{codeSchemeCodeValue}/variantmothers/")
    @ApiOperation(value = "Returns the CodeSchemes of which this CodeScheme is a variant of.", response = CodeSchemeDTO.class, responseContainer = "List")
    @ApiResponse(code = 200, message = "Return the CodeSchemes of which this CodeScheme is a variant of in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getCodeSchemeVariantMothers(@ApiParam(value = "CodeRegistry codevalue.", required = true) @PathParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                                @ApiParam(value = "CodeScheme codevalue.", required = true) @PathParam("codeSchemeCodeValue") final String codeSchemeCodeValue,
                                                @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                                @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODESCHEME, expand), pretty));
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);

        if (codeScheme == null) {
            throw new NotFoundException();
        }

        final LinkedHashSet<CodeSchemeListItem> variantMothers = codeScheme.getVariantMothersOfThisCodeScheme();
        final LinkedHashSet<CodeSchemeDTO> result = new LinkedHashSet<>();

        if (variantMothers != null && !variantMothers.isEmpty()) {
            variantMothers.forEach(variantMother -> result.add(domain.getCodeScheme(variantMother.getId().toString())));
        }

        final Meta meta = new Meta(200, null, null, null);
        meta.setResultCount(result.size());
        final ResponseWrapper<CodeSchemeDTO> wrapper = new ResponseWrapper<>();
        wrapper.setResults(result);
        wrapper.setMeta(meta);
        return Response.ok(wrapper).build();
    }

    private void filterCodes(final Set<CodeDTO> codes) {
        codes.stream().forEach(code -> code.setCodeScheme(null));
    }

    private void filterExtensions(final Set<ExtensionDTO> extensions) {
        extensions.stream().forEach(extension -> extension.setParentCodeScheme(null));
    }

    private void filterMembers(final Set<MemberDTO> members) {
        members.stream().forEach(member -> {
            member.getCode().setCodeScheme(null);
            member.setExtension(null);
        });
    }
}
