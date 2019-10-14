package fi.vm.yti.codelist.api.resource;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import fi.vm.yti.codelist.api.api.ApiUtils;
import fi.vm.yti.codelist.api.domain.Domain;
import fi.vm.yti.codelist.api.exception.NotFoundException;
import fi.vm.yti.codelist.api.exception.YtiCodeListException;
import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.CodeRegistryDTO;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ErrorModel;
import fi.vm.yti.codelist.common.dto.ExtensionDTO;
import fi.vm.yti.codelist.common.dto.MemberDTO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import static fi.vm.yti.codelist.api.util.EncodingUtils.*;

@Component
@Path("/v1/uris")
@Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", "application/xlsx", "application/csv" })
public class UriResolverResource extends AbstractBaseResource {

    private static final Logger LOG = LoggerFactory.getLogger(UriResolverResource.class);
    private static final String API_PATH_CODELIST = "/codelist";
    private static final String PATH_CODE = "code";
    private static final String PATH_EXTENSION = "extension";

    private final ApiUtils apiUtils;
    private final Domain domain;

    @Inject
    public UriResolverResource(final ApiUtils apiUtils,
                               final Domain domain) {
        this.apiUtils = apiUtils;
        this.domain = domain;
    }

    @GET
    @Path("resolve")
    @Operation(description = "Resolve URI resource.")
    @ApiResponse(responseCode = "200", description = "Resolves the API url for the given codelist resource URI.")
    @Produces({ MediaType.APPLICATION_JSON + ";charset=UTF-8", MediaType.TEXT_PLAIN })
    public Response resolveUri(@Parameter(description = "Resource URI.", required = true, in = ParameterIn.QUERY) @QueryParam("uri") final String uri) {
        final URI resolveUri = parseUriFromString(uri);
        ensureSuomiFiUriHost(resolveUri.getHost());
        final String uriPath = resolveUri.getPath();
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.registerModule(new JavaTimeModule());
        final ObjectNode json = objectMapper.createObjectNode();
        json.put("uri", uri);
        checkResourceValidity(uriPath);
        final String resourcePath = uriPath.substring(API_PATH_CODELIST.length() + 1);
        final List<String> resourceCodeValues = Arrays.asList(resourcePath.split("/"));
        json.put("url", resolveApiResourceUrl(resourceCodeValues));
        return Response.ok().entity(json).build();
    }

    @GET
    @Path("redirect")
    @Operation(description = "Redirect URI resource.")
    @Consumes({ MediaType.APPLICATION_JSON, MediaType.TEXT_HTML })
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_HTML })
    @ApiResponses(value = {
        @ApiResponse(responseCode = "303", description = "Does a redirect from codelist resource URI to codelist API."),
        @ApiResponse(responseCode = "406", description = "Resource not found."),
        @ApiResponse(responseCode = "406", description = "Cannot redirect to given URI.")
    })
    public Response redirectUri(@HeaderParam("Accept") String accept,
                                @Parameter(description = "Format for returning content.", in = ParameterIn.QUERY) @QueryParam("format") final String format,
                                @Parameter(description = "Filter string (csl) for expanding specific child resources.", in = ParameterIn.QUERY) @QueryParam("expand") final String expand,
                                @Parameter(description = "Resource URI.", required = true, in = ParameterIn.QUERY) @Encoded @QueryParam("uri") final String uri) {
        final String uriDecoded = urlDecodeString(uri);
        ensureSuomiFiUriHost(uriDecoded);
        final String uriPath = uriDecoded.substring((SUOMI_URI_HOST).length());
        checkResourceValidity(uriPath);
        final String resourcePath = uriPath.substring(API_PATH_CODELIST.length() + 1);
        final List<String> resourcePathParams = parseResourcePathIdentifiers(resourcePath);
        final List<String> acceptHeaders = parseAcceptHeaderValues(accept);
        if (format != null && !format.isEmpty()) {
            final URI redirectUrl;
            if (expand != null && !expand.isEmpty()) {
                redirectUrl = URI.create(resolveApiResourceUrl(resourcePathParams) + "?format=" + format + "&expand=" + expand);
            } else {
                redirectUrl = URI.create(resolveApiResourceUrl(resourcePathParams) + "?format=" + format);
            }
            return Response.seeOther(redirectUrl).build();
        } else if (acceptHeaders.contains(MediaType.APPLICATION_JSON)) {
            final URI redirectUrl = URI.create(resolveApiResourceUrl(resourcePathParams));
            return Response.seeOther(redirectUrl).build();
        } else {
            final URI redirectUrl = URI.create(resolveWebResourceUrl(resourcePathParams));
            return Response.seeOther(redirectUrl).build();
        }
    }

    private List<String> parseResourcePathIdentifiers(final String resourcePath) {
        if (resourcePath.contains("?")) {
            return Arrays.asList(resourcePath.substring(0, resourcePath.indexOf("?")).split("/"));
        }
        return Arrays.asList(resourcePath.split("/"));
    }

    private List<String> parseAcceptHeaderValues(final String accept) {
        final List<String> acceptHeaders = new ArrayList<>();
        for (final String acceptValue : accept.split("\\s*,\\s*")) {
            if (acceptValue.contains(";q=")) {
                acceptHeaders.add(acceptValue.substring(0, acceptValue.indexOf(";q=")));
            } else {
                acceptHeaders.add(acceptValue);
            }
        }
        return acceptHeaders;
    }

    private String resolveApiResourceUrl(final List<String> resourceCodeValues) {
        final String url;
        switch (resourceCodeValues.size()) {
            case 1: {
                final String codeRegistryCodeValue = checkNotEmpty(resourceCodeValues.get(0));
                checkCodeRegistryExists(codeRegistryCodeValue);
                url = apiUtils.createCodeRegistryUrl(codeRegistryCodeValue);
                break;
            }
            case 2: {
                final String codeRegistryCodeValue = checkNotEmpty(resourceCodeValues.get(0));
                final String codeSchemeCodeValue = checkNotEmpty(resourceCodeValues.get(1));
                checkCodeSchemeExists(codeRegistryCodeValue, codeSchemeCodeValue);
                url = apiUtils.createCodeSchemeUrl(codeRegistryCodeValue, codeSchemeCodeValue);
                break;
            }
            case 4: {
                final String codeRegistryCodeValue = checkNotEmpty(resourceCodeValues.get(0));
                final String codeSchemeCodeValue = checkNotEmpty(resourceCodeValues.get(1));
                final String pathIdentifier = checkNotEmpty(resourceCodeValues.get(2));
                if (PATH_CODE.equalsIgnoreCase(pathIdentifier)) {
                    final String codeCodeValue = checkNotEmpty(resourceCodeValues.get(3));
                    checkCodeExists(codeRegistryCodeValue, codeSchemeCodeValue, decodeDotCodeValues(codeCodeValue));
                    url = apiUtils.createCodeUrl(codeRegistryCodeValue, codeSchemeCodeValue, codeCodeValue);
                    break;
                } else if (PATH_EXTENSION.equalsIgnoreCase(pathIdentifier)) {
                    final String extensionCodeValue = checkNotEmpty(resourceCodeValues.get(3));
                    checkExtensionExists(codeRegistryCodeValue, codeSchemeCodeValue, extensionCodeValue);
                    url = apiUtils.createExtensionUrl(codeRegistryCodeValue, codeSchemeCodeValue, extensionCodeValue);
                    break;
                } else {
                    throw new YtiCodeListException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), "Codelist resource URI not resolvable!"));
                }
            }
            case 6: {
                final String codeRegistryCodeValue = checkNotEmpty(resourceCodeValues.get(0));
                final String codeSchemeCodeValue = checkNotEmpty(resourceCodeValues.get(1));
                final String extensionCodeValue = checkNotEmpty(resourceCodeValues.get(3));
                final String memberId = checkNotEmpty(resourceCodeValues.get(5));
                checkMemberExists(codeRegistryCodeValue, codeSchemeCodeValue, extensionCodeValue, memberId);
                url = apiUtils.createMemberUrl(codeRegistryCodeValue, codeSchemeCodeValue, extensionCodeValue, memberId);
                break;
            }
            default:
                throw new YtiCodeListException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), "Codelist resource URI not resolvable!"));
        }
        return url;
    }

    private String resolveWebResourceUrl(final List<String> resourceCodeValues) {
        final String url;
        switch (resourceCodeValues.size()) {
            case 1: {
                final String codeRegistryCodeValue = checkNotEmpty(resourceCodeValues.get(0));
                checkCodeRegistryExists(codeRegistryCodeValue);
                url = apiUtils.createCodeRegistryWebUrl(codeRegistryCodeValue);
                break;
            }
            case 2: {
                final String codeRegistryCodeValue = checkNotEmpty(resourceCodeValues.get(0));
                final String codeSchemeCodeValue = checkNotEmpty(resourceCodeValues.get(1));
                checkCodeSchemeExists(codeRegistryCodeValue, codeSchemeCodeValue);
                url = apiUtils.createCodeSchemeWebUrl(codeRegistryCodeValue, codeSchemeCodeValue);
                break;
            }
            case 4: {
                final String codeRegistryCodeValue = checkNotEmpty(resourceCodeValues.get(0));
                final String codeSchemeCodeValue = checkNotEmpty(resourceCodeValues.get(1));
                final String pathIdentifier = checkNotEmpty(resourceCodeValues.get(2));
                if (PATH_CODE.equalsIgnoreCase(pathIdentifier)) {
                    final String codeCodeValue = checkNotEmpty(resourceCodeValues.get(3));
                    checkCodeExists(codeRegistryCodeValue, codeSchemeCodeValue, decodeDotCodeValues(codeCodeValue));
                    url = apiUtils.createCodeWebUrl(codeRegistryCodeValue, codeSchemeCodeValue, urlEncodeCodeValue(codeCodeValue));
                    break;
                } else if (PATH_EXTENSION.equalsIgnoreCase(pathIdentifier)) {
                    final String extensionCodeValue = checkNotEmpty(resourceCodeValues.get(3));
                    checkExtensionExists(codeRegistryCodeValue, codeSchemeCodeValue, extensionCodeValue);
                    url = apiUtils.createExtensionWebUrl(codeRegistryCodeValue, codeSchemeCodeValue, extensionCodeValue);
                    break;
                } else {
                    throw new YtiCodeListException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), "Codelist resource URI not resolvable!"));
                }
            }
            case 6: {
                final String codeRegistryCodeValue = checkNotEmpty(resourceCodeValues.get(0));
                final String codeSchemeCodeValue = checkNotEmpty(resourceCodeValues.get(1));
                final String extensionCodeValue = checkNotEmpty(resourceCodeValues.get(3));
                final String memberId = checkNotEmpty(resourceCodeValues.get(5));
                checkMemberExists(codeRegistryCodeValue, codeSchemeCodeValue, extensionCodeValue, memberId);
                url = apiUtils.createMemberWebUrl(codeRegistryCodeValue, codeSchemeCodeValue, extensionCodeValue, memberId);
                break;
            }
            default:
                throw new YtiCodeListException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), "Codelist resource URI not resolvable!"));
        }
        return url;
    }

    private void checkResourceValidity(final String uriPath) {
        final String resourcePath = uriPath.substring(API_PATH_CODELIST.length() + 1);
        final List<String> resourceCodeValues = Arrays.asList(resourcePath.split("/"));
        if (!uriPath.toLowerCase().startsWith(API_PATH_CODELIST)) {
            LOG.error("Codelist resource URI not resolvable, wrong context path!");
            throw new YtiCodeListException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), "Codelist resource URI not resolvable, wrong context path!"));
        } else if (resourceCodeValues.isEmpty()) {
            LOG.error("Codelist resource URI not resolvable, empty resource path!");
            throw new YtiCodeListException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), "Codelist resource URI not resolvable, empty resource path!"));
        }
    }

    private String checkNotEmpty(final String string) {
        if (string != null && !string.isEmpty()) {
            return string;
        } else {
            LOG.error("Resource hook not valid due to empty resource ID.");
            throw new YtiCodeListException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), "Resource hook not valid due to empty resource ID."));
        }
    }

    private void checkCodeRegistryExists(final String codeRegistryCodeValue) {
        final CodeRegistryDTO codeRegistry = domain.getCodeRegistry(codeRegistryCodeValue);
        if (codeRegistry == null) {
            throw new NotFoundException();
        }
    }

    private void checkCodeSchemeExists(final String codeRegistryCodeValue,
                                       final String codeSchemeCodeValue) {
        final CodeSchemeDTO codeScheme = domain.getCodeScheme(codeRegistryCodeValue, codeSchemeCodeValue);
        if (codeScheme == null) {
            throw new NotFoundException();
        }
    }

    private void checkCodeExists(final String codeRegistryCodeValue,
                                 final String codeSchemeCodeValue,
                                 final String codeCodeValue) {
        final CodeDTO code = domain.getCode(codeRegistryCodeValue, codeSchemeCodeValue, codeCodeValue);
        if (code == null) {
            throw new NotFoundException();
        }
    }

    private void checkExtensionExists(final String codeRegistryCodeValue,
                                      final String codeSchemeCodeValue,
                                      final String extensionCodeValue) {
        final ExtensionDTO extension = domain.getExtension(codeRegistryCodeValue, codeSchemeCodeValue, extensionCodeValue);
        if (extension == null) {
            throw new NotFoundException();
        }
    }

    private void checkMemberExists(final String codeRegistryCodeValue,
                                   final String codeSchemeCodeValue,
                                   final String extensionCodeValue,
                                   final String memberId) {
        final ExtensionDTO extension = domain.getExtension(codeRegistryCodeValue, codeSchemeCodeValue, extensionCodeValue);
        if (extension != null) {
            final MemberDTO member = domain.getMember(memberId, extensionCodeValue);
            if (member == null) {
                throw new NotFoundException();
            }
        } else {
            throw new NotFoundException();
        }
    }
}
