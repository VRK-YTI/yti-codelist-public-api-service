package fi.vm.yti.codelist.api.resource;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterInjector;

import fi.vm.yti.codelist.api.domain.Domain;
import fi.vm.yti.codelist.api.exception.NotFoundException;
import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import static fi.vm.yti.codelist.common.constants.ApiConstants.FILTER_NAME_CODE;

/**
 * REST resources for Codes.
 */
@Component
@Path("/v1/codes")
@Api(value = "codes")
@Produces({MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv"})
public class CodeResource extends AbstractBaseResource {

    private final Domain domain;

    @Inject
    public CodeResource(final Domain domain) {
        this.domain = domain;
    }

    @GET
    @Path("{codeId}")
    @ApiOperation(value = "Return one specific Code.", response = CodeDTO.class)
    @ApiResponse(code = 200, message = "Returns one specific Code in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    public Response getCode(@ApiParam(value = "Code Id.", required = true) @PathParam("codeId") final String codeId,
                            @ApiParam(value = "Filter string (csl) for expanding specific child resources.") @QueryParam("expand") final String expand) {
        ObjectWriterInjector.set(new AbstractBaseResource.FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODE, expand)));
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeId);
        if (codeScheme != null) {
            return Response.ok(codeScheme).build();
        } else {
            throw new NotFoundException();
        }
    }
}
