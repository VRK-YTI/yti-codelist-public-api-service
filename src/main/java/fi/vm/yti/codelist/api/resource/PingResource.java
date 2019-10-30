package fi.vm.yti.codelist.api.resource;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import org.springframework.stereotype.Component;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;

@Component
@Path("/ping")
@Produces("text/plain")
@Tag(name = "Health")
public class PingResource extends AbstractBaseResource {

    @GET
    @Operation(description = "Return pong upon successful API request.")
    @ApiResponse(responseCode = "200", description = "Returns the String 'pong'.")
    @Produces("text/plain")
    public Response ping() {
        return Response.ok("pong").build();
    }
}
