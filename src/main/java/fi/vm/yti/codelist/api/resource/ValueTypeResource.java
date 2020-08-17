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
import fi.vm.yti.codelist.api.export.ValueTypeExporter;
import fi.vm.yti.codelist.common.dto.Meta;
import fi.vm.yti.codelist.common.dto.ValueTypeDTO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
@Path("/v1/valuetypes")
@Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "text/csv" })
@Tag(name = "ValueType")
public class ValueTypeResource extends AbstractBaseResource {

    private final Domain domain;
    private final ValueTypeExporter valueTypeExporter;

    @Inject
    public ValueTypeResource(final Domain domain,
                             final ValueTypeExporter valueTypeExporter) {
        this.domain = domain;
        this.valueTypeExporter = valueTypeExporter;
    }

    @GET
    @Operation(description = "Return a list of available ValueTypes.")
    @ApiResponse(responseCode = "200", description = "Returns all ValueTypes in specified format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "text/csv" })
    public Response getValueTypes(@Parameter(description = "Pagination parameter for page size.", in = ParameterIn.QUERY) @QueryParam("pageSize") final Integer pageSize,
                                  @Parameter(description = "Pagination parameter for start index.", in = ParameterIn.QUERY) @QueryParam("from") @DefaultValue("0") final Integer from,
                                  @Parameter(description = "ValueType localName as string value.", in = ParameterIn.QUERY) @QueryParam("localName") final String localName,
                                  @Parameter(description = "Format for content.", in = ParameterIn.QUERY) @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                  @Parameter(description = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("after") final String after,
                                  @Parameter(description = "Before date filtering parameter, results will be codes with modified date before this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("before") final String before,
                                  @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                  @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        final Meta meta = new Meta(200, pageSize, from, parseDateFromString(after), parseDateFromString(before));
        final Set<ValueTypeDTO> valueTypes = domain.getValueTypes(localName, meta);
        if (FORMAT_CSV.equalsIgnoreCase(format)) {
            final String csv = valueTypeExporter.createCsv(valueTypes);
            return streamCsvValueTypesOutput(csv);
        } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            final Workbook workbook = valueTypeExporter.createExcel(valueTypes, format);
            return streamExcelValueTypesOutput(workbook);
        } else {
            ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_PROPERTYTYPE, expand), pretty));
            final ResponseWrapper<ValueTypeDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(valueTypes);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        }
    }

    @GET
    @Path("{valueTypeIdentifier}")
    @Operation(description = "Return one specific ValueType.")
    @ApiResponse(responseCode = "200", description = "Returns one specific ValueType in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getValueType(@Parameter(description = "ValueType ID.", in = ParameterIn.PATH, required = true) @PathParam("valueTypeIdentifier") final String valueTypeIdentifier,
                                 @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                 @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_PROPERTYTYPE, expand), pretty));
        final ValueTypeDTO valueType = domain.getValueType(valueTypeIdentifier);
        if (valueType != null) {
            return Response.ok(valueType).build();
        } else {
            throw new NotFoundException();
        }
    }
}
