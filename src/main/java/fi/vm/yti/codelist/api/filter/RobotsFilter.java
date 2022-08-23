package fi.vm.yti.codelist.api.filter;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;

import fi.vm.yti.codelist.api.configuration.PublicApiServiceProperties;

@Provider
public class RobotsFilter implements ContainerResponseFilter {

    private static final String DEV_DOMAIN = ".yti.cloud.dvv.fi";

    private final PublicApiServiceProperties publicApiServiceProperties;

    @Inject
    public RobotsFilter(final PublicApiServiceProperties publicApiServiceProperties) {
        this.publicApiServiceProperties = publicApiServiceProperties;
    }

    @Override
    public void filter(final ContainerRequestContext requestContext,
                       final ContainerResponseContext responseContext) {
        if (publicApiServiceProperties.getHost().contains(DEV_DOMAIN)) {
            responseContext.getHeaders().add("X-Robots-Tag", "none");
        }
        responseContext.getHeaders().add("Strict-Transport-Security", "max-age=31536000");
    }
}
