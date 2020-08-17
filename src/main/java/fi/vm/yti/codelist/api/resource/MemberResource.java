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
import fi.vm.yti.codelist.api.export.MemberExporter;
import fi.vm.yti.codelist.common.dto.MemberDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
@Path("/v1/members")
@Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "text/csv" })
@Tag(name = "Member")
public class MemberResource extends AbstractBaseResource {

    private final Domain domain;
    private final MemberExporter memberExporter;

    @Inject
    public MemberResource(final Domain domain,
                          final MemberExporter memberExporter) {
        this.domain = domain;
        this.memberExporter = memberExporter;
    }

    @GET
    @Operation(description = "Return list of available Members.")
    @ApiResponse(responseCode = "200", description = "Returns all Members in specified format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", MediaType.TEXT_PLAIN })
    public Response getMembers(@Parameter(description = "Pagination parameter for page size.", in = ParameterIn.QUERY) @QueryParam("pageSize") final Integer pageSize,
                               @Parameter(description = "Pagination parameter for start index.", in = ParameterIn.QUERY) @QueryParam("from") @DefaultValue("0") final Integer from,
                               @Parameter(description = "Format for content.", in = ParameterIn.QUERY) @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                               @Parameter(description = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("after") final String after,
                               @Parameter(description = "Before date filtering parameter, results will be codes with modified date before this ISO 8601 formatted date string.", in = ParameterIn.QUERY) @QueryParam("before") final String before,
                               @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                               @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        final Meta meta = new Meta(200, pageSize, from, parseDateFromString(after), parseDateFromString(before));
        final Set<MemberDTO> members = domain.getMembers(meta);
        if (FORMAT_CSV.startsWith(format.toLowerCase())) {
            final String csv = memberExporter.createCsv(null, members);
            return streamCsvMembersOutput(csv);
        } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            final Workbook workbook = memberExporter.createExcel(null, members, format);
            return streamExcelMembersOutput(workbook);
        } else {
            ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_MEMBER, expand), pretty));
            final ResponseWrapper<MemberDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(members);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        }
    }

    @GET
    @Path("{memberId}")
    @Operation(description = "Return one specific Member.")
    @ApiResponse(responseCode = "200", description = "Returns one specific Member in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getMember(@Parameter(description = "Member UUID or sequenceId depending on the context of the call.", in = ParameterIn.PATH, required = true) @PathParam("memberId") final String memberId,
                              @Parameter(description = "Member's extension's codeValue", required = true , in = ParameterIn.QUERY) @QueryParam("extensionCodeValue") final String extensionCodeValue,
                              @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                              @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_MEMBER, expand), pretty));
        final MemberDTO member = domain.getMember(memberId, extensionCodeValue);
        if (member != null) {
            return Response.ok(member).build();
        } else {
            throw new NotFoundException();
        }
    }
}
