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
import fi.vm.yti.codelist.api.export.ExtensionSchemeExporter;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ExtensionSchemeDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
@Path("/v1/extensionschemes")
@Api(value = "extensionschemes")
@Produces({MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv"})
public class ExtensionSchemeResource extends AbstractBaseResource {

    private final Domain domain;
    private final ExtensionSchemeExporter extensionSchemeExporter;

    @Inject
    public ExtensionSchemeResource(final Domain domain,
                                   final ExtensionSchemeExporter extensionExporter) {
        this.domain = domain;
        this.extensionSchemeExporter = extensionExporter;
    }

    @GET
    @ApiOperation(value = "Return list of available ExtensionSchemes.", response = CodeSchemeDTO.class, responseContainer = "List")
    @ApiResponse(code = 200, message = "Returns all ExtensionSchemes in specified format.")
    @Produces({MediaType.APPLICATION_JSON + ";charset=UTF-8", MediaType.TEXT_PLAIN})
    public Response getExtensionSchemes(@ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                        @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                        @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                        @ApiParam(value = "ExtensionScheme PrefLabel.") @QueryParam("prefLabel") final String prefLabel,
                                        @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                        @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand) {
        if (FORMAT_CSV.startsWith(format.toLowerCase())) {
            final Set<ExtensionSchemeDTO> extensionSchemes = domain.getExtensionSchemes(pageSize, from, prefLabel, null, Meta.parseAfterFromString(after), null);
            final String csv = extensionSchemeExporter.createCsv(extensionSchemes);
            return streamCsvExtensionSchemesOutput(csv);
        } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            final Set<ExtensionSchemeDTO> extensionSchemes = domain.getExtensionSchemes(pageSize, from, prefLabel, null, Meta.parseAfterFromString(after), null);
            final Workbook workbook = extensionSchemeExporter.createExcel(extensionSchemes, format);
            return streamExcelExtensionSchemesOutput(workbook);
        } else {
            final Meta meta = new Meta(200, null, null, after);
            ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_EXTENSIONSCHEME, expand)));
            final Set<ExtensionSchemeDTO> extensionSchemes = domain.getExtensionSchemes(pageSize, from, prefLabel, null, meta.getAfter(), meta);
            meta.setResultCount(extensionSchemes.size());
            final ResponseWrapper<ExtensionSchemeDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(extensionSchemes);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        }
    }

    @GET
    @Path("{extensionId}")
    @ApiOperation(value = "Return one specific Extension.", response = CodeSchemeDTO.class)
    @ApiResponse(code = 200, message = "Returns one specific Extension in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getExtensionScheme(@ApiParam(value = "ExtensionScheme UUID.", required = true) @PathParam("extensionId") final String extensionId,
                                       @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_EXTENSIONSCHEME, expand)));
        final ExtensionSchemeDTO extensionScheme = domain.getExtensionScheme(extensionId);
        if (extensionScheme != null) {
            return Response.ok(extensionScheme).build();
        } else {
            throw new NotFoundException();
        }
    }
}
