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
import fi.vm.yti.codelist.api.export.ExtensionExporter;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ExtensionDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
@Path("/v1/extensions")
@Api(value = "extensions")
@Produces({MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv"})
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
    @ApiOperation(value = "Return list of available Extensions.", response = CodeSchemeDTO.class, responseContainer = "List")
    @ApiResponse(code = 200, message = "Returns all Extensions in specified format.")
    @Produces({MediaType.APPLICATION_JSON + ";charset=UTF-8", MediaType.TEXT_PLAIN})
    public Response getExtensions(@ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                  @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                  @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                  @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                  @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand) {
        if (FORMAT_CSV.startsWith(format.toLowerCase())) {
            final Set<ExtensionDTO> extensions = domain.getExtensions(pageSize, from, null, Meta.parseAfterFromString(after), null);
            final String csv = extensionExporter.createCsv(extensions);
            return streamCsvExtensionsOutput(csv);
        } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            final Set<ExtensionDTO> extensions = domain.getExtensions(pageSize, from, null, Meta.parseAfterFromString(after), null);
            final Workbook workbook = extensionExporter.createExcel(extensions, format);
            return streamExcelExtensionsOutput(workbook);
        } else {
            final Meta meta = new Meta(200, null, null, after);
            ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_EXTENSION, expand)));
            final Set<ExtensionDTO> extensions = domain.getExtensions(pageSize, from, null, meta.getAfter(), meta);
            meta.setResultCount(extensions.size());
            final ResponseWrapper<ExtensionDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(extensions);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        }
    }

    @GET
    @Path("{extensionId}")
    @ApiOperation(value = "Return one specific Extension.", response = CodeSchemeDTO.class)
    @ApiResponse(code = 200, message = "Returns one specific Extension in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getCodeScheme(@ApiParam(value = "Extension UUID.", required = true) @PathParam("extensionId") final String extensionId,
                                  @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_EXTENSION, expand)));
        final ExtensionDTO extension = domain.getExtension(extensionId);
        if (extension != null) {
            return Response.ok(extension).build();
        } else {
            throw new NotFoundException();
        }
    }
}
