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
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.cfg.ObjectWriterInjector;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.api.api.ResponseWrapper;
import fi.vm.yti.codelist.api.domain.Domain;
import fi.vm.yti.codelist.api.exception.NotFoundException;
import fi.vm.yti.codelist.api.export.CodeSchemeExporter;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;
import static java.util.Arrays.asList;

@Component
@Path("/v1/codeschemes")
@Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
@Tag(name = "CodeScheme")
public class CodeSchemeResource extends AbstractBaseResource {

    private final Domain domain;
    private final CodeSchemeExporter codeSchemeExporter;

    @Inject
    public CodeSchemeResource(final Domain domain,
                              final CodeSchemeExporter codeSchemeExporter) {
        this.domain = domain;
        this.codeSchemeExporter = codeSchemeExporter;
    }

    @GET
    @Operation(description = "Return list of available CodeSchemes.")
    @ApiResponse(responseCode = "200", description = "Returns all CodeSchemes in specified format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", MediaType.TEXT_PLAIN })
    public Response getCodeSchemes(@Parameter(description = "CodeRegistry CodeValue.", in = ParameterIn.QUERY) @QueryParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                   @Parameter(description = "CodeRegistry Name.", in = ParameterIn.QUERY) @QueryParam("codeRegistryName") final String codeRegistryPrefLabel,
                                   @Parameter(description = "Pagination parameter for page size.", in = ParameterIn.QUERY) @QueryParam("pageSize") final Integer pageSize,
                                   @Parameter(description = "Pagination parameter for start index.", in = ParameterIn.QUERY) @QueryParam("from") @DefaultValue("0") final Integer from,
                                   @Parameter(description = "Service information domain classifications in CSL format.", in = ParameterIn.QUERY) @QueryParam("infoDomain") final String infoDomain,
                                   @Parameter(description = "CodeScheme codeValue as string value.", in = ParameterIn.QUERY) @QueryParam("codeValue") final String codeSchemeCodeValue,
                                   @Parameter(description = "CodeScheme PrefLabel as string value.", in = ParameterIn.QUERY) @QueryParam("prefLabel") final String codeSchemePrefLabel,
                                   @Parameter(description = "Language code for sorting results.", in = ParameterIn.QUERY) @QueryParam("language") @DefaultValue("fi") final String language,
                                   @Parameter(description = "Search term for matching codeValue and prefLabel.", in = ParameterIn.QUERY) @QueryParam("searchTerm") final String searchTerm,
                                   @Parameter(description = "Boolean that controls is search also matches codes' codeValues and prefLabels inside CodeSchemes.", in = ParameterIn.QUERY) @QueryParam("searchCodes") @DefaultValue("false") final Boolean searchCodes,
                                   @Parameter(description = "Boolean that controls is search also matches extensions' codeValues and prefLabels inside CodeSchemes.", in = ParameterIn.QUERY) @QueryParam("searchExtensions") @DefaultValue("false") final Boolean searchExtensions,
                                   @Parameter(description = "Status enumerations in CSL format.", in = ParameterIn.QUERY) @QueryParam("status") final String status,
                                   @Parameter(description = "Extension PropertyType localName as string value for searching.", in = ParameterIn.QUERY) @QueryParam("extensionPropertyType") final String extensionPropertyType,
                                   @Parameter(description = "Format for content.", in = ParameterIn.QUERY) @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                   @Parameter(description = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("after") final String after,
                                   @Parameter(description = "Before date filtering parameter, results will be codes with modified date before this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("before") final String before,
                                   @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                   @Parameter(description = "Sort mode for response values.", in = ParameterIn.QUERY) @QueryParam("sortMode") @DefaultValue("default") final String sortMode,
                                   @Parameter(description = "Organizations filtering parameter, results will be codeschemes belonging to these organizations", in = ParameterIn.QUERY) @QueryParam("organizations") final String organizationsCsv,
                                   @Parameter(description = "User organizations filtering parameter, for filtering unfinished code schemes", in = ParameterIn.QUERY) @QueryParam("userOrganizations") final String userOrganizationsCsv,
                                   @Parameter(description = "Include INCOMPLETE statused code schemes.", in = ParameterIn.QUERY) @QueryParam("includeIncomplete") @DefaultValue("false") final Boolean includeIncomplete,
                                   @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        final Meta meta = new Meta(200, pageSize, from, after, before);
        final List<String> infoDomainsList = parseInfoDomains(infoDomain);
        final List<String> organizations = organizationsCsv == null ? null : asList(organizationsCsv.toLowerCase().split(","));
        final List<String> userOrganizations = userOrganizationsCsv == null ? null : asList(userOrganizationsCsv.toLowerCase().split(","));
        final List<String> statusList = parseStatus(status);
        if (FORMAT_CSV.startsWith(format.toLowerCase())) {
            final Set<CodeSchemeDTO> codeSchemes = domain.getCodeSchemes(sortMode, organizations, userOrganizations, includeIncomplete, codeRegistryCodeValue, codeRegistryPrefLabel, codeSchemeCodeValue, codeSchemePrefLabel, language, searchTerm, searchCodes, searchExtensions, statusList, infoDomainsList, extensionPropertyType, meta);
            final String csv = codeSchemeExporter.createCsv(codeSchemes);
            return streamCsvCodeSchemesOutput(csv);
        } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            final Set<CodeSchemeDTO> codeSchemes = domain.getCodeSchemes(sortMode, organizations, userOrganizations, includeIncomplete, codeRegistryCodeValue, codeRegistryPrefLabel, codeSchemeCodeValue, codeSchemePrefLabel, language, searchTerm, searchCodes, searchExtensions, statusList, infoDomainsList, extensionPropertyType, meta);
            final Workbook workbook = codeSchemeExporter.createExcel(codeSchemes, format);
            return streamExcelCodeSchemesOutput(workbook);
        } else {
            ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODESCHEME, expand), pretty));
            final Set<CodeSchemeDTO> codeSchemes = domain.getCodeSchemes(sortMode, organizations, userOrganizations, includeIncomplete, codeRegistryCodeValue, codeRegistryPrefLabel, codeSchemeCodeValue, codeSchemePrefLabel, language, searchTerm, searchCodes, searchExtensions, statusList, infoDomainsList, extensionPropertyType, meta);
            final ResponseWrapper<CodeSchemeDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(codeSchemes);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        }
    }

    @GET
    @Path("{codeSchemeId}")
    @Operation(description = "Return one specific CodeScheme.")
    @ApiResponse(responseCode = "200", description = "Returns one specific CodeScheme in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getCodeScheme(@Parameter(description = "CodeScheme CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("codeSchemeId") final String codeSchemeId,
                                  @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                  @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODESCHEME, expand), pretty));
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeSchemeId);
        if (codeScheme != null) {
            return Response.ok(codeScheme).build();
        } else {
            throw new NotFoundException();
        }
    }
}
