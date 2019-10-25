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
import fi.vm.yti.codelist.api.export.ExtensionExporter;
import fi.vm.yti.codelist.common.dto.ExtensionDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
@Path("/v1/extensions")
@Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
public class ExtensionResource extends AbstractBaseResource {

    private final Domain domain;
    private final ExtensionExporter extensionExporter;

    @Inject
    public ExtensionResource(final Domain domain,
                             final ExtensionExporter extensionExporter) {
        this.domain = domain;
        this.extensionExporter = extensionExporter;
    }

    @GET
    @Operation(description = "Return list of available Extensions.")
    @ApiResponse(responseCode = "200", description = "Returns all Extensions in specified format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", MediaType.TEXT_PLAIN })
    public Response getExtensions(@Parameter(description = "Pagination parameter for page size.", in = ParameterIn.QUERY) @QueryParam("pageSize") final Integer pageSize,
                                  @Parameter(description = "Pagination parameter for start index.", in = ParameterIn.QUERY) @QueryParam("from") @DefaultValue("0") final Integer from,
                                  @Parameter(description = "Format for content.", in = ParameterIn.QUERY) @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                  @Parameter(description = "Extension PrefLabel.", in = ParameterIn.QUERY) @QueryParam("prefLabel") final String prefLabel,
                                  @Parameter(description = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("after") final String after,
                                  @Parameter(description = "Before date filtering parameter, results will be codes with modified date before this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("before") final String before,
                                  @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                  @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        final Meta meta = new Meta(200, pageSize, from, after, before);
        final Set<ExtensionDTO> extensions = domain.getExtensions(prefLabel, meta);
        if (FORMAT_CSV.startsWith(format.toLowerCase())) {
            final String csv = extensionExporter.createCsv(extensions);
            return streamCsvExtensionsOutput(csv);
        } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            final Workbook workbook = extensionExporter.createExcel(extensions, format);
            return streamExcelExtensionsOutput(workbook);
        } else {
            ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_EXTENSION, expand), pretty));
            final ResponseWrapper<ExtensionDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(extensions);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        }
    }

    @GET
    @Path("{extensionId}")
    @Operation(description = "Return one specific Extension.")
    @ApiResponse(responseCode = "200", description = "Returns one specific Extension in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getExtension(@Parameter(description = "Extension UUID.", in = ParameterIn.PATH, required = true) @PathParam("extensionId") final String extensionId,
                                 @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                 @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_EXTENSION, expand), pretty));
        final ExtensionDTO extension = domain.getExtension(extensionId);
        if (extension != null) {
            return Response.ok(extension).build();
        } else {
            throw new NotFoundException();
        }
    }
}
