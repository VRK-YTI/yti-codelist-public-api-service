package fi.vm.yti.codelist.api.api;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.api.configuration.FrontendProperties;
import fi.vm.yti.codelist.api.configuration.PublicApiServiceProperties;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
public class ApiUtils {

    private final PublicApiServiceProperties publicApiServiceProperties;
    private final FrontendProperties frontendProperties;

    @Inject
    public ApiUtils(final PublicApiServiceProperties publicApiServiceProperties,
                    final FrontendProperties frontendProperties) {
        this.publicApiServiceProperties = publicApiServiceProperties;
        this.frontendProperties = frontendProperties;
    }

    public String getPublicApiServiceHostname() {
        final StringBuilder builder = new StringBuilder();
        final String port = publicApiServiceProperties.getPort();
        builder.append(publicApiServiceProperties.getHost());
        if (port != null && port.length() > 0) {
            builder.append(":");
            builder.append(port);
        }
        return builder.toString();
    }

    /**
     * Creates the next page resource URL for fetching more content from this resource.
     *
     * @param apiVersion The REST API version string.
     * @param apiPath    The path for the API resource.
     * @param after      After timestamp in ISO 8601 format for filtering content.
     * @param pageSize   Page size used in pagination.
     * @param from       Start index for pagination.
     * @return The next page URL.
     */
    public String createNextPageUrl(final String apiVersion,
                                    final String apiPath,
                                    final String after,
                                    final Integer pageSize,
                                    final Integer from) {
        final String port = publicApiServiceProperties.getPort();
        final StringBuilder builder = new StringBuilder();
        builder.append(publicApiServiceProperties.getScheme());
        builder.append("://");
        builder.append(publicApiServiceProperties.getHost());
        if (port != null && port.length() > 0) {
            builder.append(":");
            builder.append(port);
        }
        builder.append(publicApiServiceProperties.getContextPath());
        builder.append(API_BASE_PATH);
        builder.append("/");
        builder.append(apiVersion);
        builder.append(apiPath);
        builder.append("/");
        builder.append("?pageSize=");
        builder.append(pageSize);
        builder.append("&from=");
        builder.append(from);
        if (after != null && !after.isEmpty()) {
            builder.append("&after=");
            builder.append(after);
        }

        return builder.toString();
    }

    public String createCodeRegistryUrl(final String codeRegistryCodeValue) {
        return createResourceUrl(API_PATH_CODEREGISTRIES, codeRegistryCodeValue);
    }

    public String createCodeSchemeUrl(final String codeRegistryCodeValue,
                                      final String codeSchemeCodeValue) {
        return createResourceUrl(API_PATH_CODEREGISTRIES + "/" + codeRegistryCodeValue + API_PATH_CODESCHEMES, codeSchemeCodeValue);
    }

    public String createCodeUrl(final String codeRegistryCodeValue,
                                final String codeSchemeCodeValue,
                                final String codeCodeValue) {
        return createResourceUrl(API_PATH_CODEREGISTRIES + "/" + codeRegistryCodeValue + API_PATH_CODESCHEMES + "/" + codeSchemeCodeValue + API_PATH_CODES, codeCodeValue);
    }

    public String createCodeRegistryWebUrl(final String codeRegistryCodeValue) {
        return createFrontendBaseUrl() + "/registry;registryCode=" + codeRegistryCodeValue;
    }

    public String createCodeSchemeWebUrl(final String codeRegistryCodeValue,
                                         final String codeSchemeCodeValue) {
        return createFrontendBaseUrl() + "/codescheme;registryCode=" + codeRegistryCodeValue + ";schemeCode=" + codeSchemeCodeValue;
    }

    public String createCodeWebUrl(final String codeRegistryCodeValue,
                                   final String codeSchemeCodeValue,
                                   final String codeCodeValue) {
        return createFrontendBaseUrl() + "/code;registryCode=" + codeRegistryCodeValue + ";schemeCode=" + codeSchemeCodeValue + ";codeCode=" + codeCodeValue;
    }

    private String createResourceUrl(final String apiPath,
                                     final String resourceId) {
        final StringBuilder builder = new StringBuilder();

        builder.append(createBaseUrl());
        builder.append(publicApiServiceProperties.getContextPath());
        builder.append(API_BASE_PATH);
        builder.append("/");
        builder.append(API_VERSION);
        builder.append(apiPath);
        builder.append("/");
        if (resourceId != null && !resourceId.isEmpty()) {
            builder.append(resourceId);
        }

        return builder.toString();
    }

    private String createBaseUrl() {
        final String port = publicApiServiceProperties.getPort();

        final StringBuilder builder = new StringBuilder();

        builder.append(publicApiServiceProperties.getScheme());
        builder.append("://");
        builder.append(publicApiServiceProperties.getHost());
        if (port != null && port.length() > 0) {
            builder.append(":");
            builder.append(port);
        }

        return builder.toString();
    }

    private String createFrontendBaseUrl() {
        final String port = frontendProperties.getPort();

        final StringBuilder builder = new StringBuilder();

        builder.append(frontendProperties.getScheme());
        builder.append("://");
        builder.append(frontendProperties.getHost());
        if (port != null && port.length() > 0) {
            builder.append(":");
            builder.append(port);
        }

        return builder.toString();
    }
}
