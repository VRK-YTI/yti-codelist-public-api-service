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
import fi.vm.yti.codelist.api.export.ExternalReferenceExporter;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ExternalReferenceDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

/**
 * REST resources for ExternalReferenceDTOs.
 */
@Component
@Path("/v1/externalreferences")
@Api(value = "externalreferences")
@Produces({MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv"})
public class ExternalReferenceResource extends AbstractBaseResource {

    private static final Logger LOG = LoggerFactory.getLogger(ExternalReferenceResource.class);
    private final Domain domain;
    private final ExternalReferenceExporter externalReferenceExporter;

    @Inject
    public ExternalReferenceResource(final Domain domain,
                                     final ExternalReferenceExporter exportService) {
        this.domain = domain;
        this.externalReferenceExporter = exportService;
    }

    @GET
    @ApiOperation(value = "Return a list of available ExternalReferences.", response = ExternalReferenceDTO.class, responseContainer = "List")
    @ApiResponse(code = 200, message = "Returns all ExternalReferences in specified format.")
    @Produces({MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv"})
    public Response getExternalReferenceDTOs(@ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                                             @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                                             @ApiParam(value = "ExternalReference name as string value.") @QueryParam("name") final String name,
                                             @ApiParam(value = "CodeSchemeDTO id.") @QueryParam("codeSchemeId") final String codeSchemeId,
                                             @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                                             @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                                             @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand) {
        logApiRequest(LOG, METHOD_GET, API_PATH_VERSION_V1, API_PATH_EXTERNALREFERENCES);
        CodeSchemeDTO codeScheme = null;
        if (codeSchemeId != null && !codeSchemeId.isEmpty()) {
            codeScheme = domain.getCodeScheme(codeSchemeId);
            if (codeScheme == null) {
                final ResponseWrapper<ExternalReferenceDTO> wrapper = new ResponseWrapper<>();
                final Meta meta = new Meta();
                wrapper.setMeta(meta);
                meta.setCode(404);
                meta.setMessage("No such resource.");
                return Response.status(Response.Status.NOT_FOUND).entity(wrapper).build();
            }
        }
        if (FORMAT_CSV.equalsIgnoreCase(format)) {
            final Set<ExternalReferenceDTO> externalReferences = domain.getExternalReferences(pageSize, from, name, codeScheme, Meta.parseAfterFromString(after), null);
            final String csv = externalReferenceExporter.createCsv(externalReferences);
            return streamCsvExternalReferenceDTOsOutput(csv);
        } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            final Set<ExternalReferenceDTO> externalReferences = domain.getExternalReferences(pageSize, from, name, codeScheme, Meta.parseAfterFromString(after), null);
            final Workbook workbook = externalReferenceExporter.createExcel(externalReferences, format);
            return streamExcelExternalReferenceDTOsOutput(workbook);
        } else {
            final Meta meta = new Meta(200, null, null, after);
            ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_EXTERNALREFERENCE, expand)));
            final Set<ExternalReferenceDTO> externalReferences = domain.getExternalReferences(pageSize, from, name, codeScheme, meta.getAfter(), meta);
            meta.setResultCount(externalReferences.size());
            final ResponseWrapper<ExternalReferenceDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(externalReferences);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        }
    }

    @GET
    @Path("{externalReferenceId}")
    @ApiOperation(value = "Return one specific ExternalReference.", response = ExternalReferenceDTO.class)
    @ApiResponse(code = 200, message = "Returns one specific ExternalReference in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getExternalReferenceDTO(@ApiParam(value = "ExternalReference CodeValue.", required = true) @PathParam("externalReferenceId") final String externalReferenceId,
                                            @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand) {
        logApiRequest(LOG, METHOD_GET, API_PATH_VERSION_V1, API_PATH_EXTERNALREFERENCES + "/" + externalReferenceId + "/");
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_EXTERNALREFERENCE, expand)));
        final ExternalReferenceDTO externalReference = domain.getExternalReference(externalReferenceId);
        if (externalReference != null) {
            return Response.ok(externalReference).build();
        } else {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }
}
