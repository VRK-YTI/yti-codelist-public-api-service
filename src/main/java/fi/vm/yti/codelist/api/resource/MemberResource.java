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
import fi.vm.yti.codelist.api.export.MemberExporter;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.MemberDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
@Path("/v1/members")
@Api(value = "members")
@Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
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
    @ApiOperation(value = "Return list of available Members.", response = CodeSchemeDTO.class, responseContainer = "List")
    @ApiResponse(code = 200, message = "Returns all Members in specified format.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", MediaType.TEXT_PLAIN })
    public Response getMembers(@ApiParam(value = "Pagination parameter for page size.") @QueryParam("pageSize") final Integer pageSize,
                               @ApiParam(value = "Pagination parameter for start index.") @QueryParam("from") @DefaultValue("0") final Integer from,
                               @ApiParam(value = "Format for content.") @QueryParam("format") @DefaultValue(FORMAT_JSON) final String format,
                               @ApiParam(value = "After date filtering parameter, results will be codes with modified date after this ISO 8601 formatted date string.") @QueryParam("after") final String after,
                               @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand) {
        if (FORMAT_CSV.startsWith(format.toLowerCase())) {
            final Set<MemberDTO> members = domain.getMembers(pageSize, from, Meta.parseAfterFromString(after), null);
            final String csv = memberExporter.createCsv(members);
            return streamCsvMembersOutput(csv);
        } else if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format) || FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            final Set<MemberDTO> members = domain.getMembers(pageSize, from, Meta.parseAfterFromString(after), null);
            final Workbook workbook = memberExporter.createExcel(members, format);
            return streamExcelMembersOutput(workbook);
        } else {
            final Meta meta = new Meta(200, pageSize, from, after);
            ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_MEMBER, expand)));
            final Set<MemberDTO> members = domain.getMembers(pageSize, from, meta.getAfter(), meta);
            meta.setResultCount(members.size());
            final ResponseWrapper<MemberDTO> wrapper = new ResponseWrapper<>();
            wrapper.setResults(members);
            wrapper.setMeta(meta);
            return Response.ok(wrapper).build();
        }
    }

    @GET
    @Path("{memberId}")
    @ApiOperation(value = "Return one specific Member.", response = CodeSchemeDTO.class)
    @ApiResponse(code = 200, message = "Returns one specific Member in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getMember(@ApiParam(value = "Member UUID.", required = true) @PathParam("memberId") final String memberId,
                              @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_MEMBER, expand)));
        final MemberDTO member = domain.getMember(memberId);
        if (member != null) {
            return Response.ok(member).build();
        } else {
            throw new NotFoundException();
        }
    }
}
