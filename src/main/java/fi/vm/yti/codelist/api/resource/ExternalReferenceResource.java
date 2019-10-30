package fi.vm.yti.codelist.api.resource;

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
import fi.vm.yti.codelist.api.export.ExternalReferenceExporter;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ExternalReferenceDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
@Path("/v1/externalreferences")
@Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
@Tag(name = "ExternalReference")
public class ExternalReferenceResource extends AbstractBaseResource {

    private final Domain domain;
    private final ExternalReferenceExporter externalReferenceExporter;

    @Inject
    public ExternalReferenceResource(final Domain domain,
                                     final ExternalReferenceExporter exportService) {
        this.domain = domain;
        this.externalReferenceExporter = exportService;
    }

    @GET
    @Operation(description = "Return a list of available ExternalReferences.")
    @ApiResponse(responseCode = "200", description = "Returns all ExternalReferences in specified format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
    public Response getExternalReferences(@Parameter(description = "Pagination parameter for page size.", in = ParameterIn.QUERY) @QueryParam("pageSize") final Integer pageSize,
                                          @Parameter(description = "Pagination parameter for start index.", in = ParameterIn.QUERY) @QueryParam("from") @DefaultValue("0") final Integer from,
                                          @Parameter(description = "ExternalReference name as string value.", in = ParameterIn.QUERY) @QueryParam("name") final String name,
                                          @Parameter(description = "CodeScheme id.", in = ParameterIn.QUERY) @QueryParam("codeSchemeId") final String codeSchemeId,
                                          @Parameter(description = "Return all links from the system.", in = ParameterIn.QUERY) @QueryParam("all") @DefaultValue("false") final Boolean all,
                                          @Parameter(description = "Format for content.", in = ParameterIn.QUERY) @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                          @Parameter(description = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("after") final String after,
                                          @Parameter(description = "Before date filtering parameter, results will be codes with modified date before this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("before") final String before,
                                          @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                          @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        CodeSchemeDTO codeScheme = null;
        if (codeSchemeId != null && !codeSchemeId.isEmpty()) {
            codeScheme = domain.getCodeScheme(codeSchemeId);
            if (codeScheme == null) {
                throw new NotFoundException();
            }
        }
        final Meta meta = new Meta(200, pageSize, from, after, before);
        final Set<ExternalReferenceDTO> externalReferences = domain.getExternalReferences(name, codeScheme, all, meta);
        if (FORMAT_CSV.equalsIgnoreCase(format)) {
            final String csv = externalReferenceExporter.createCsv(externalReferences);
            return streamCsvExternalReferencesOutput(csv);
        } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            final Workbook workbook = externalReferenceExporter.createExcel(externalReferences, format);
            return streamExcelExternalReferencesOutput(workbook);
        } else {
            ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_EXTERNALREFERENCE, expand), pretty));
            final ResponseWrapper<ExternalReferenceDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(externalReferences);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        }
    }

    @GET
    @Path("{externalReferenceId}")
    @Operation(description = "Return one specific ExternalReference.")
    @ApiResponse(responseCode = "200", description = "Returns one specific ExternalReference in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getExternalReference(@Parameter(description = "ExternalReference CodeValue.", in = ParameterIn.PATH, required = true) @PathParam("externalReferenceId") final String externalReferenceId,
                                         @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                         @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_EXTERNALREFERENCE, expand), pretty));
        final ExternalReferenceDTO externalReference = domain.getExternalReference(externalReferenceId);
        if (externalReference != null) {
            return Response.ok(externalReference).build();
        } else {
            throw new NotFoundException();
        }
    }
}
