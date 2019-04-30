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
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterInjector;

import fi.vm.yti.codelist.api.api.ResponseWrapper;
import fi.vm.yti.codelist.api.domain.Domain;
import fi.vm.yti.codelist.api.exception.NotFoundException;
import fi.vm.yti.codelist.api.export.ValueTypeExporter;
import fi.vm.yti.codelist.common.dto.Meta;
import fi.vm.yti.codelist.common.dto.ValueTypeDTO;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
@Path("/v1/valuetypes")
@Api(value = "valuetypes")
@Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
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
    @ApiOperation(value = "Return a list of available ValueTypes.", response = ValueTypeDTO.class, responseContainer = "List")
    @ApiResponse(code = 200, message = "Returns all ValueTypes in specified format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
    public Response getValueTypes(@ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                  @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                  @ApiParam(value = "ValueType localName as string value.") @QueryParam("localName") final String localName,
                                  @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                  @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                  @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                  @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
        if (FORMAT_CSV.equalsIgnoreCase(format)) {
            final Set<ValueTypeDTO> valueTypes = domain.getValueTypes(pageSize, from, localName, Meta.parseAfterFromString(after), null);
            final String csv = valueTypeExporter.createCsv(valueTypes);
            return streamCsvValueTypesOutput(csv);
        } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            final Set<ValueTypeDTO> valueTypes = domain.getValueTypes(pageSize, from, localName, Meta.parseAfterFromString(after), null);
            final Workbook workbook = valueTypeExporter.createExcel(valueTypes, format);
            return streamExcelValueTypesOutput(workbook);
        } else {
            final Meta meta = new Meta(200, pageSize, from, after);
            ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_PROPERTYTYPE, expand), pretty));
            final Set<ValueTypeDTO> valueTypes = domain.getValueTypes(pageSize, from, localName, meta.getAfter(), meta);
            meta.setResultCount(valueTypes.size());
            final ResponseWrapper<ValueTypeDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(valueTypes);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        }
    }

    @GET
    @Path("{valueTypeIdentifier}")
    @ApiOperation(value = "Return one specific ValueType.", response = ValueTypeDTO.class)
    @ApiResponse(code = 200, message = "Returns one specific ValueType in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getValueType(@ApiParam(value = "ValueType ID.", required = true) @PathParam("valueTypeIdentifier") final String valueTypeIdentifier,
                                 @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand,
                                 @ApiParam(value = "Pretty format JSON output.") @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_PROPERTYTYPE, expand), pretty));
        final ValueTypeDTO valueType = domain.getValueType(valueTypeIdentifier);
        if (valueType != null) {
            return Response.ok(valueType).build();
        } else {
            throw new NotFoundException();
        }
    }
}
