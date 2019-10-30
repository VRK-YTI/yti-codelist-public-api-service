package fi.vm.yti.codelist.api.resource;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.jackson.internal.jackson.jaxrs.cfg.ObjectWriterInjector;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.api.domain.Domain;
import fi.vm.yti.codelist.api.exception.NotFoundException;
import fi.vm.yti.codelist.common.dto.CodeDTO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import static fi.vm.yti.codelist.common.constants.ApiConstants.FILTER_NAME_CODE;

@Component
@Path("/v1/codes")
@Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
public class CodeResource extends AbstractBaseResource {

    private final Domain domain;

    @Inject
    public CodeResource(final Domain domain) {
        this.domain = domain;
    }

    @GET
    @Path("{codeId}")
    @Operation(description = "Return one specific Code.")
    @ApiResponse(responseCode = "200", description = "Returns one specific Code in JSON format.")
    @Produces(MediaType.APPLICATION_JSON + ";charset=UTF-8")
    @Tag(name = "Code")
    public Response getCode(@Parameter(description = "Code Id.", in = ParameterIn.PATH, required = true) @PathParam("codeId") final String codeId,
                            @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                            @Parameter(description = "Pretty format JSON output.", in = ParameterIn.QUERY) @QueryParam("pretty") final String pretty) {
        ObjectWriterInjector.set(new FilterModifier(createSimpleFilterProvider(FILTER_NAME_CODE, expand), pretty));
        final CodeDTO code = domain.getCode(codeId);
        if (code != null) {
            return Response.ok(code).build();
        } else {
            throw new NotFoundException();
        }
    }
}
