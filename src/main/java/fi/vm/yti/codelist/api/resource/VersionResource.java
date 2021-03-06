package fi.vm.yti.codelist.api.resource;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.api.configuration.VersionInformation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;

@Component
@Path("/version")
@Produces("text/plain")
@Tag(name = "System")
public class VersionResource extends AbstractBaseResource {

    private final VersionInformation versionInformation;

    public VersionResource(final VersionInformation versionInformation) {
        this.versionInformation = versionInformation;
    }

    @GET
    @Operation(description = "Get version information")
    @ApiResponse(responseCode = "200", description = "Returns the version of the running Public API Service application.")
    public String getVersionInformation() {
        return "\n" +
            "          __  .__                      ___.   .__  .__        \n" +
            " ___.__._/  |_|__|         ______  __ _\\_ |__ |  | |__| ____  \n" +
            "<   |  |\\   __\\  |  ______ \\____ \\|  |  \\ __ \\|  | |  |/ ___\\ \n" +
            " \\___  | |  | |  | /_____/ |  |_> >  |  / \\_\\ \\  |_|  \\  \\___ \n" +
            " / ____| |__| |__|         |   __/|____/|___  /____/__|\\___  >\n" +
            " \\/                        |__|             \\/             \\/ \n" +
            "              .__                            .__              \n" +
            "_____  ______ |__|   ______ ______________  _|__| ____  ____  \n" +
            "\\__  \\ \\____ \\|  |  /  ___// __ \\_  __ \\  \\/ /  |/ ___\\/ __ \\ \n" +
            " / __ \\|  |_> >  |  \\___ \\\\  ___/|  | \\/\\   /|  \\  \\__\\  ___/ \n" +
            "(____  /   __/|__| /____  >\\___  >__|    \\_/ |__|\\___  >___  >\n" +
            "     \\/|__|             \\/     \\/                    \\/    \\/ \n" +
            "\n" +
            "                --- Version " + versionInformation.getVersion() + " running. --- \n";
    }
}
