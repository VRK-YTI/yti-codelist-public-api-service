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

import fi.vm.yti.codelist.api.api.ResponseWrapper;
import fi.vm.yti.codelist.api.domain.Domain;
import fi.vm.yti.codelist.api.export.CodeSchemeExporter;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;
import static java.util.Arrays.asList;

/**
 * REST resources for CodeSchemes.
 */
@Component
@Path("/v1/codeschemes")
@Api(value = "codeschemes")
@Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
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
    @ApiOperation(value = "Return list of available CodeSchemes.", response = CodeSchemeDTO.class, responseContainer = "List")
    @ApiResponse(code = 200, message = "Returns all CodeSchemes in specified format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", MediaType.TEXT_PLAIN })
    public Response getCodeSchemes(@ApiParam(value = "CodeRegistry CodeValue.") @QueryParam("codeRegistryCodeValue") final String codeRegistryCodeValue,
                                   @ApiParam(value = "CodeRegistry Name.") @QueryParam("codeRegistryName") final String codeRegistryPrefLabel,
                                   @ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                   @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                   @ApiParam(value = "Service classifications in CSL format.") @QueryParam("dataClassification") final String dataClassification,
                                   @ApiParam(value = "CodeScheme codeValue as string value.") @QueryParam("codeValue") final String codeSchemeCodeValue,
                                   @ApiParam(value = "CodeScheme PrefLabel as string value.") @QueryParam("prefLabel") final String codeSchemePrefLabel,
                                   @ApiParam(value = "Language code for sorting results.") @QueryParam("language") @DefaultValue("fi") final String language,
                                   @ApiParam(value = "Search term for matching codeValue and prefLabel.") @QueryParam("searchTerm") final String searchTerm,
                                   @ApiParam(value = "Boolean that controls is search also matches codes codeValues and prefLabels inside CodeSchemes.") @QueryParam("searchCodes") @DefaultValue("false") final Boolean searchCodes,
                                   @ApiParam(value = "Status enumerations in CSL format.") @QueryParam("status") final String status,
                                   @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                   @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                   @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                   @ApiParam(value = "Sort mode for response values.") @QueryParam("sortMode") @DefaultValue("default") final String sortMode,
                                   @ApiParam(value = "Organizations filtering parameter, results will be codeschemes belonging to these organizations") @QueryParam("organizations") final String organizationsCsv,
                                   @ApiParam(value = "User organizations filtering parameter, for filtering unfinished code schemes") @QueryParam("userOrganizations") final String userOrganizationsCsv) {
        final List<String> dataClassificationList = parseDataClassifications(dataClassification);
        final List<String> organizations = organizationsCsv == null ? null : asList(organizationsCsv.toLowerCase().split(","));
        final List<String> userOrganizations = userOrganizationsCsv == null ? null : asList(userOrganizationsCsv.toLowerCase().split(","));
        final List<String> statusList = parseStatus(status);
        if (FORMAT_CSV.startsWith(format.toLowerCase())) {
            final Set<CodeSchemeDTO> codeSchemes = domain.getCodeSchemes(pageSize, from, sortMode, organizations, userOrganizations, codeRegistryCodeValue, codeRegistryPrefLabel, codeSchemeCodeValue, codeSchemePrefLabel, language, searchTerm, searchCodes, statusList, dataClassificationList, Meta.parseAfterFromString(after), null);
            final String csv = codeSchemeExporter.createCsv(codeSchemes);
            return streamCsvCodeSchemesOutput(csv);
        } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            final Set<CodeSchemeDTO> codeSchemes = domain.getCodeSchemes(pageSize, from, sortMode, organizations, userOrganizations, codeRegistryCodeValue, codeRegistryPrefLabel, codeSchemeCodeValue, codeSchemePrefLabel, language, searchTerm, searchCodes, statusList, dataClassificationList, Meta.parseAfterFromString(after), null);
            final Workbook workbook = codeSchemeExporter.createExcel(codeSchemes, format);
            return streamExcelCodeSchemesOutput(workbook);
        } else {
            final Meta meta = new Meta(200, pageSize, from, after);
            ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODESCHEME, expand)));
            final Set<CodeSchemeDTO> codeSchemes = domain.getCodeSchemes(pageSize, from, sortMode, organizations, userOrganizations, codeRegistryCodeValue, codeRegistryPrefLabel, codeSchemeCodeValue, codeSchemePrefLabel, language, searchTerm, searchCodes, statusList, dataClassificationList, meta.getAfter(), meta);
            meta.setResultCount(codeSchemes.size());
            final ResponseWrapper<CodeSchemeDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(codeSchemes);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        }
    }

    @GET
    @Path("{codeSchemeId}")
    @ApiOperation(value = "Return one specific CodeScheme.", response = CodeSchemeDTO.class)
    @ApiResponse(code = 200, message = "Returns one specific CodeScheme in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getCodeScheme(@ApiParam(value = "CodeScheme CodeValue.", required = true) @PathParam("codeSchemeId") final String codeSchemeId,
                                  @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODESCHEME, expand)));
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeSchemeId);
        if (codeScheme != null) {
            return Response.ok(codeScheme).build();
        } else {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }
}
