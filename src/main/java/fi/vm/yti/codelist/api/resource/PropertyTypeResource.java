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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterInjector;

import fi.vm.yti.codelist.api.api.ResponseWrapper;
import fi.vm.yti.codelist.api.domain.Domain;
import fi.vm.yti.codelist.api.export.PropertyTypeExporter;
import fi.vm.yti.codelist.common.dto.Meta;
import fi.vm.yti.codelist.common.dto.PropertyTypeDTO;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

/**
 * REST resources for PropertyTypes.
 */
@Component
@Path("/v1/propertytypes")
@Api(value = "propertytypes")
@Produces({MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv"})
public class PropertyTypeResource extends AbstractBaseResource {

    private static final Logger LOG = LoggerFactory.getLogger(PropertyTypeResource.class);
    private final Domain domain;
    private final PropertyTypeExporter propertyTypeExporter;

    @Inject
    public PropertyTypeResource(final Domain domain,
                                final PropertyTypeExporter propertyTypeExporter) {
        this.domain = domain;
        this.propertyTypeExporter = propertyTypeExporter;
    }

    @GET
    @ApiOperation(value = "Return a list of available PropertyTypes.", response = PropertyTypeDTO.class, responseContainer = "List")
    @ApiResponse(code = 200, message = "Returns all PropertyTypes in specified format.")
    @Produces({MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv"})
    public Response getPropertyTypes(@ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                     @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                     @ApiParam(value = "PropertyType name as string value.") @QueryParam("name") final String name,
                                     @ApiParam(value = "Context name as string value.") @QueryParam("context") final String context,
                                     @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                     @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                     @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand) {
        logApiRequest(LOG, METHOD_GET, API_PATH_VERSION_V1, API_PATH_PROPERTYTYPES);
        if (FORMAT_CSV.equalsIgnoreCase(format)) {
            final Set<PropertyTypeDTO> propertyTypes = domain.getPropertyTypes(pageSize, from, name, context, Meta.parseAfterFromString(after), null);
            final String csv = propertyTypeExporter.createCsv(propertyTypes);
            return streamCsvPropertyTypeDTOsOutput(csv);
        } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            final Set<PropertyTypeDTO> propertyTypes = domain.getPropertyTypes(pageSize, from, name, context, Meta.parseAfterFromString(after), null);
            final Workbook workbook = propertyTypeExporter.createExcel(propertyTypes, format);
            return streamExcelPropertyTypeDTOsOutput(workbook);
        } else {
            final Meta meta = new Meta(200, null, null, after);
            ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_PROPERTYTYPE, expand)));
            final Set<PropertyTypeDTO> propertyTypes = domain.getPropertyTypes(pageSize, from, name, context, meta.getAfter(), meta);
            meta.setResultCount(propertyTypes.size());
            final ResponseWrapper<PropertyTypeDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(propertyTypes);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        }
    }

    @GET
    @Path("{propertyTypeId}")
    @ApiOperation(value = "Return one specific PropertyType.", response = PropertyTypeDTO.class)
    @ApiResponse(code = 200, message = "Returns one specific PropertyType in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getPropertyTypeDTO(@ApiParam(value = "PropertyType CodeValue.", required = true) @PathParam("propertyTypeId") final String propertyTypeId,
                                       @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand) {
        logApiRequest(LOG, METHOD_GET, API_PATH_VERSION_V1, API_PATH_PROPERTYTYPES + "/" + propertyTypeId + "/");
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_PROPERTYTYPE, expand)));
        final PropertyTypeDTO propertyType = domain.getPropertyType(propertyTypeId);
        if (propertyType != null) {
            return Response.ok(propertyType).build();
        } else {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }
}
