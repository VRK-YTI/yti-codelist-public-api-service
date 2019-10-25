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
import fi.vm.yti.codelist.api.export.PropertyTypeExporter;
import fi.vm.yti.codelist.common.dto.Meta;
import fi.vm.yti.codelist.common.dto.PropertyTypeDTO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
@Path("/v1/propertytypes")
@Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
public class PropertyTypeResource extends AbstractBaseResource {

    private final Domain domain;
    private final PropertyTypeExporter propertyTypeExporter;

    @Inject
    public PropertyTypeResource(final Domain domain,
                                final PropertyTypeExporter propertyTypeExporter) {
        this.domain = domain;
        this.propertyTypeExporter = propertyTypeExporter;
    }

    @GET
    @Operation(description = "Return a list of available PropertyTypes.")
    @ApiResponse(responseCode = "200", description = "Returns all PropertyTypes in specified format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
    public Response getPropertyTypes(@Parameter(description = "Pagination parameter for page size.", in = ParameterIn.QUERY) @QueryParam("pageSize") final Integer pageSize,
                                     @Parameter(description = "Pagination parameter for start index.", in = ParameterIn.QUERY) @QueryParam("from") @DefaultValue("0") final Integer from,
                                     @Parameter(description = "PropertyType name as string value.", in = ParameterIn.QUERY) @QueryParam("name") final String name,
                                     @Parameter(description = "Context name as string value.", in = ParameterIn.QUERY) @QueryParam("context") final String context,
                                     @Parameter(description = "Language code for sorting results.", in = ParameterIn.QUERY) @QueryParam("language") final String language,
                                     @Parameter(description = "Type as string value.", in = ParameterIn.QUERY) @QueryParam("type") final String type,
                                     @Parameter(description = "Format for content.", in = ParameterIn.QUERY) @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                     @Parameter(description = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("after") final String after,
                                     @Parameter(description = "Before date filtering parameter, results will be codes with modified date before this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("before") final String before,
                                     @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                     @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        final Meta meta = new Meta(200, pageSize, from, after, before);
        final Set<PropertyTypeDTO> propertyTypes = domain.getPropertyTypes(name, context, language, type, meta);
        if (FORMAT_CSV.equalsIgnoreCase(format)) {
            final String csv = propertyTypeExporter.createCsv(propertyTypes);
            return streamCsvPropertyTypesOutput(csv);
        } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            final Workbook workbook = propertyTypeExporter.createExcel(propertyTypes, format);
            return streamExcelPropertyTypesOutput(workbook);
        } else {
            ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_PROPERTYTYPE, expand), pretty));
            final ResponseWrapper<PropertyTypeDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(propertyTypes);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        }
    }

    @GET
    @Path("{propertyTypeIdentifier}")
    @Operation(description = "Return one specific PropertyType.")
    @ApiResponse(responseCode = "200", description = "Returns one specific PropertyType in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getPropertyType(@Parameter(description = "PropertyType ID.", in = ParameterIn.PATH, required = true) @PathParam("propertyTypeIdentifier") final String propertyTypeIdentifier,
                                    @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                    @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_PROPERTYTYPE, expand), pretty));
        final PropertyTypeDTO propertyType = domain.getPropertyType(propertyTypeIdentifier);
        if (propertyType != null) {
            return Response.ok(propertyType).build();
        } else {
            throw new NotFoundException();
        }
    }
}
