package fi.vm.yti.codelist.api.filter;

import java.util.List;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

@Provider
public class RequestLoggingFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private static final Logger LOG = LoggerFactory.getLogger(RequestLoggingFilter.class);

    @Context
    private ResourceInfo resourceInfo;

    @Override
    public void filter(final ContainerRequestContext requestContext) {
        MDC.put("startTime", String.valueOf(System.currentTimeMillis()));

        LOG.debug("*** Start request logging ***");
        LOG.debug("Resource: /{}", requestContext.getUriInfo().getPath());
        LOG.debug("Class: {}", resourceInfo.getResourceClass().getCanonicalName());
        LOG.debug("Method: {}", resourceInfo.getResourceMethod().getName());
        logQueryParameters(requestContext);
        logRequestHeader(requestContext);
    }

    private void logQueryParameters(final ContainerRequestContext requestContext) {
        LOG.debug("*** Start query parameters section of request ***");
        requestContext.getUriInfo().getQueryParameters().keySet().forEach(parameterName -> {
            final List<String> paramList = requestContext.getUriInfo().getQueryParameters().get(parameterName);
            paramList.forEach(paramValue -> LOG.debug("Parameter: {}, Value: {}", parameterName, paramValue));
        });
        LOG.debug("*** End query parameters section of request ***");
    }

    private void logRequestHeader(final ContainerRequestContext requestContext) {
        LOG.debug("*** Start header section of request ***");
        LOG.debug("Method type: {}", requestContext.getMethod());
        requestContext.getHeaders().keySet().forEach(headerName -> {
            final String headerValue;
            if ("Authorization".equalsIgnoreCase(headerName) || "cookie".equalsIgnoreCase(headerName)) {
                headerValue = "[PROTECTED]";
            } else {
                headerValue = requestContext.getHeaderString(headerName);
            }
            if ("User-Agent".equalsIgnoreCase(headerName)) {
                MDC.put("userAgent", headerValue);
            } else if ("Host".equalsIgnoreCase(headerName)) {
                MDC.put("host", headerValue);
            }
            LOG.debug("Header: {}, Value: {} ", headerName, headerValue);
        });
        LOG.debug("*** End header section of request ***");
    }

    @Override
    public void filter(final ContainerRequestContext requestContext,
                       final ContainerResponseContext responseContext) {
        final Long executionTime = getExecutionTime();
        if (executionTime == null) {
            return;
        }
        LOG.debug("Request execution time: {} ms", executionTime);
        LOG.debug("*** End request logging ***");
        logRequestInfo(requestContext, responseContext, executionTime);
        MDC.clear();
    }

    private Long getExecutionTime() {
        final String startTimeString = MDC.get("startTime");
        if (startTimeString != null && !startTimeString.isEmpty()) {
            final long startTime = Long.parseLong(startTimeString);
            return System.currentTimeMillis() - startTime;
        }
        return null;
    }

    private void logRequestInfo(final ContainerRequestContext requestContext,
                                final ContainerResponseContext responseContext,
                                final long executionTime) {
        final String log = "Request: /" +
            requestContext.getMethod() + " " + requestContext.getUriInfo().getPath() + ", " +
            "Status: " + responseContext.getStatus() + ", " +
            "User-Agent: " + MDC.get("userAgent") + ", " +
            "Host: " + MDC.get("host") + ", " +
            "Time: " + executionTime + " ms";
        LOG.info(log);
    }
}
