package fi.vm.yti.codelist.api.domain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import fi.vm.yti.codelist.api.dto.ResourceDTO;
import fi.vm.yti.codelist.api.exception.JsonParsingException;
import fi.vm.yti.codelist.api.exception.YtiCodeListException;
import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.CodeRegistryDTO;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ErrorModel;
import fi.vm.yti.codelist.common.dto.ExtensionDTO;
import fi.vm.yti.codelist.common.dto.ExternalReferenceDTO;
import fi.vm.yti.codelist.common.dto.MemberDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import fi.vm.yti.codelist.common.dto.PropertyTypeDTO;
import fi.vm.yti.codelist.common.dto.ValueTypeDTO;
import fi.vm.yti.codelist.common.model.Status;
import static fi.vm.yti.codelist.api.exception.ErrorConstants.ERR_MSG_USER_406;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;
import static java.lang.Math.toIntExact;
import static org.elasticsearch.index.query.QueryBuilders.*;

@Singleton
@Service
public class DomainImpl implements Domain {

    private static final Logger LOG = LoggerFactory.getLogger(DomainImpl.class);
    private static final int MAX_SIZE = 50000;
    private static final String TEXT_ANALYZER = "text_analyzer";
    private static final String BOOSTSTATUS = "boostStatus";
    private static final Set<String> sortLanguages = new HashSet<>(Arrays.asList(LANGUAGE_CODE_FI,
        LANGUAGE_CODE_EN,
        LANGUAGE_CODE_SV));
    private final Client client;

    @Inject
    private DomainImpl(final Client client) {
        this.client = client;
    }

    private void registerModulesToMapper(ObjectMapper mapper) {
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,
            false);
    }

    public CodeRegistryDTO getCodeRegistry(final String codeRegistryCodeValue) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_CODEREGISTRY).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODEREGISTRY)
                .setTypes(ELASTIC_TYPE_CODEREGISTRY)
                .addSort("codeValue.raw",
                    SortOrder.ASC);
            final BoolQueryBuilder builder = boolQuery()
                .should(matchQuery("id",
                    codeRegistryCodeValue.toLowerCase()))
                .should(matchQuery("codeValue",
                    codeRegistryCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER))
                .minimumShouldMatch(1);
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            if (response.getHits().getTotalHits() > 0) {
                final SearchHit hit = response.getHits().getAt(0);
                LOG.debug(String.format("Found %d CodeRegistries",
                    response.getHits().getTotalHits()));
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(),
                            CodeRegistryDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getCodeRegistry reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            }
        }
        return null;
    }

    public Set<CodeRegistryDTO> getCodeRegistries() {
        return getCodeRegistries(MAX_SIZE,
            0,
            null,
            null,
            null,
            null,
            null);
    }

    public Set<CodeRegistryDTO> getCodeRegistries(final Integer pageSize,
                                                  final Integer from,
                                                  final String codeRegistryCodeValue,
                                                  final String codeRegistryPrefLabel,
                                                  final Date after,
                                                  final Meta meta,
                                                  final List<String> organizations) {
        validatePageSize(pageSize);
        final Set<CodeRegistryDTO> codeRegistries = new LinkedHashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_CODEREGISTRY).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODEREGISTRY)
                .setTypes(ELASTIC_TYPE_CODEREGISTRY)
                .addSort("codeValue.raw",
                    SortOrder.ASC)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0);

            final BoolQueryBuilder builder = constructSearchQuery(codeRegistryCodeValue,
                codeRegistryPrefLabel,
                after);
            if (organizations != null && !organizations.isEmpty()) {
                builder.must(termsQuery("organizations.id.keyword",
                    organizations));
            }
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta,
                response);
            response.getHits().forEach(hit -> {
                LOG.debug(String.format("Found %d CodeRegistries",
                    response.getHits().getTotalHits()));
                try {
                    codeRegistries.add(mapper.readValue(hit.getSourceAsString(),
                        CodeRegistryDTO.class));
                } catch (final IOException e) {
                    LOG.error("getCodeRegistries reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            });
        }
        return codeRegistries;
    }

    public CodeSchemeDTO getCodeScheme(final String codeSchemeId) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_CODESCHEME).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODESCHEME)
                .setTypes(ELASTIC_TYPE_CODESCHEME)
                .addSort("codeValue.raw",
                    SortOrder.ASC);
            final BoolQueryBuilder builder = boolQuery()
                .must(matchQuery("id",
                    codeSchemeId.toLowerCase()));
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            if (response.getHits().getTotalHits() > 0) {
                LOG.debug(String.format("Found %d CodeSchemes",
                    response.getHits().getTotalHits()));
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(),
                            CodeSchemeDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getCodeScheme reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            }
        }
        return null;
    }

    public CodeSchemeDTO getCodeScheme(final String codeRegistryCodeValue,
                                       final String codeSchemeCodeValue) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_CODESCHEME).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODESCHEME)
                .setTypes(ELASTIC_TYPE_CODESCHEME)
                .addSort("codeValue.raw",
                    SortOrder.ASC);
            final BoolQueryBuilder builder = boolQuery()
                .should(matchQuery("id",
                    codeSchemeCodeValue.toLowerCase()))
                .should(matchQuery("codeValue",
                    codeSchemeCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER))
                .minimumShouldMatch(1);
            builder.must(matchQuery("codeRegistry.codeValue",
                codeRegistryCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER));
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            if (response.getHits().getTotalHits() > 0) {
                LOG.debug(String.format("Found %d CodeSchemes",
                    response.getHits().getTotalHits()));
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(),
                            CodeSchemeDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getCodeScheme reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            }
        }
        return null;
    }

    public Set<CodeSchemeDTO> getCodeSchemesByCodeRegistryCodeValue(final String codeRegistryCodeValue,
                                                                    final List<String> organizations,
                                                                    final List<String> userOrganizationIds,
                                                                    final String language) {
        return getCodeSchemes(MAX_SIZE,
            0,
            null,
            organizations,
            userOrganizationIds,
            codeRegistryCodeValue,
            null,
            null,
            null,
            language,
            null,
            false,
            false,
            null,
            null,
            null,
            null,
            null);
    }

    public Set<CodeSchemeDTO> getCodeSchemes(final String language) {
        return getCodeSchemes(MAX_SIZE,
            0,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            language,
            null,
            false,
            false,
            null,
            null,
            null,
            null,
            null);
    }

    private Set<String> getCodeSchemesMatchingCodes(final String searchTerm) {
        final Set<String> codeSchemeUuids = new HashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_CODE).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODE)
                .setTypes(ELASTIC_TYPE_CODE);
            final BoolQueryBuilder builder = boolQuery();
            if (searchTerm != null) {
                builder.should(prefixQuery("codeValue",
                    searchTerm.toLowerCase()));
                builder.should(nestedQuery("prefLabel",
                    multiMatchQuery(searchTerm.toLowerCase() + "*",
                        "prefLabel.*").type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX),
                    ScoreMode.None));
                builder.minimumShouldMatch(1);
            }
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            response.getHits().forEach(hit -> {
                try {
                    codeSchemeUuids.add(mapper.readValue(hit.getSourceAsString(),
                        CodeDTO.class).getCodeScheme().getId().toString().toLowerCase());
                } catch (final IOException e) {
                    LOG.error("getCodeSchemesMatchingCodes reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            });
            return codeSchemeUuids;
        }
        return codeSchemeUuids;
    }

    private Set<String> getCodeSchemesMatchingExtensions(final String searchTerm) {
        final Set<String> codeSchemeUuids = new HashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_EXTENSION).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_EXTENSION)
                .setTypes(ELASTIC_TYPE_EXTENSION);
            final BoolQueryBuilder builder = boolQuery();
            if (searchTerm != null) {
                if (searchTerm != null && !searchTerm.isEmpty()) {
                    builder.should(prefixQuery("codeValue",
                        searchTerm.toLowerCase()));
                    builder.should(nestedQuery("prefLabel",
                        multiMatchQuery(searchTerm.toLowerCase() + "*",
                            "prefLabel.*").type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX),
                        ScoreMode.None));
                }
                builder.minimumShouldMatch(1);
            }
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            response.getHits().forEach(hit -> {
                try {
                    codeSchemeUuids.add(mapper.readValue(hit.getSourceAsString(),
                        ExtensionDTO.class).getParentCodeScheme().getId().toString().toLowerCase());
                } catch (final IOException e) {
                    LOG.error("getCodeSchemesMatchingExtensions reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            });
            return codeSchemeUuids;
        }
        return codeSchemeUuids;
    }

    public Set<CodeSchemeDTO> getCodeSchemes(final Integer pageSize,
                                             final Integer from,
                                             final String sortMode,
                                             final List<String> organizationIds,
                                             final List<String> userOrganizationIds,
                                             final String codeRegistryCodeValue,
                                             final String codeRegistryPrefLabel,
                                             final String codeSchemeCodeValue,
                                             final String codeSchemePrefLabel,
                                             final String language,
                                             final String searchTerm,
                                             final boolean searchCodes,
                                             final boolean searchExtensions,
                                             final List<String> statuses,
                                             final List<String> infoDomains,
                                             final String extensionPropertyType,
                                             final Date after,
                                             final Meta meta) {
        validatePageSize(pageSize);
        final Set<String> codeSchemeUuids = new HashSet<>();
        if (searchCodes) {
            codeSchemeUuids.addAll(getCodeSchemesMatchingCodes(searchTerm));
        }
        if (searchExtensions) {
            codeSchemeUuids.addAll(getCodeSchemesMatchingExtensions(searchTerm));
        }
        final Set<CodeSchemeDTO> codeSchemes = new LinkedHashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_CODESCHEME).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODESCHEME)
                .setTypes(ELASTIC_TYPE_CODESCHEME)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0);
            final BoolQueryBuilder builder = boolQuery();
            if (searchTerm != null && !searchTerm.isEmpty()) {
                final BoolQueryBuilder boolQueryBuilder = boolQuery();
                boolQueryBuilder.should(prefixQuery("codeValue",
                    searchTerm.toLowerCase()));
                boolQueryBuilder.should(nestedQuery("prefLabel",
                    multiMatchQuery(searchTerm.toLowerCase(),
                        "prefLabel.*").type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX),
                    ScoreMode.None));
                if (!codeSchemeUuids.isEmpty()) {
                    boolQueryBuilder.should(termsQuery("id",
                        codeSchemeUuids));
                }
                boolQueryBuilder.minimumShouldMatch(1);
                builder.must(boolQueryBuilder);
            }
            if (codeSchemeCodeValue != null && !codeSchemeCodeValue.isEmpty()) {
                builder.must(prefixQuery("codeValue",
                    codeSchemeCodeValue.toLowerCase()));
            }
            if (codeSchemePrefLabel != null && !codeSchemePrefLabel.isEmpty()) {
                builder.must(nestedQuery("prefLabel",
                    multiMatchQuery(codeSchemePrefLabel.toLowerCase(),
                        "prefLabel.*").type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX),
                    ScoreMode.None));
            }
            if (after != null) {
                final ISO8601DateFormat dateFormat = new ISO8601DateFormat();
                final String afterString = dateFormat.format(after);
                builder.must(rangeQuery("modified").gt(afterString));
            }
            if (organizationIds != null && !organizationIds.isEmpty()) {
                builder.must(nestedQuery("organizations",
                    termsQuery("organizations.id.keyword",
                        organizationIds),
                    ScoreMode.None));
            }
            if (codeRegistryCodeValue != null && !codeRegistryCodeValue.isEmpty()) {
                builder.must(matchQuery("codeRegistry.codeValue",
                    codeRegistryCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER));
            }
            if (codeRegistryPrefLabel != null && !codeRegistryPrefLabel.isEmpty()) {
                builder.must(nestedQuery("codeRegistry.prefLabel",
                    multiMatchQuery(codeRegistryPrefLabel.toLowerCase(),
                        "prefLabel.*").type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX),
                    ScoreMode.None));
            }
            if (infoDomains != null && !infoDomains.isEmpty()) {
                builder.must(nestedQuery("infoDomains",
                    matchQuery("infoDomains.codeValue",
                        infoDomains),
                    ScoreMode.None));
            }
            if (extensionPropertyType != null) {
                builder.must(nestedQuery("extensions",
                    matchQuery("extensions.propertyType.localName",
                        extensionPropertyType),
                    ScoreMode.None));
            }
            if (BOOSTSTATUS.equalsIgnoreCase(sortMode)) {
                searchRequest.addSort(SortBuilders.scoreSort());
                boostStatus(builder);
            }
            if (language != null && !language.isEmpty()) {
                searchRequest.addSort(SortBuilders.fieldSort("prefLabel." + language + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                sortLanguages.forEach(sortLanguage -> {
                    if (!language.equalsIgnoreCase(sortLanguage)) {
                        searchRequest.addSort(SortBuilders.fieldSort("prefLabel." + sortLanguage + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                    }
                });
                searchRequest.addSort("codeValue.raw",
                    SortOrder.ASC);
            } else {
                searchRequest.addSort("codeValue.raw",
                    SortOrder.ASC);
            }
            if (statuses != null && !statuses.isEmpty()) {
                final BoolQueryBuilder boolQueryBuilder = boolQuery();
                if (statuses.contains(Status.INCOMPLETE.toString())) {
                    final BoolQueryBuilder unfinishedQueryBuilder = boolQuery();
                    unfinishedQueryBuilder.must(matchQuery("status.keyword",
                        Status.INCOMPLETE.toString()));
                    unfinishedQueryBuilder.must(nestedQuery("organizations",
                        termsQuery("organizations.id.keyword",
                            userOrganizationIds),
                        ScoreMode.None));
                    boolQueryBuilder.should(unfinishedQueryBuilder);
                    statuses.remove(Status.INCOMPLETE.toString());
                }
                boolQueryBuilder.should(termsQuery("status.keyword",
                    statuses));
                builder.must(termsQuery("status.keyword",
                    statuses));
                boolQueryBuilder.minimumShouldMatch(1);
                builder.must(boolQueryBuilder);
            } else {
                final BoolQueryBuilder boolQueryBuilder = boolQuery();
                boolQueryBuilder.should(termsQuery("status.keyword",
                    getRegularStatuses()));
                if (userOrganizationIds != null && !userOrganizationIds.isEmpty()) {
                    final BoolQueryBuilder unfinishedQueryBuilder = boolQuery();
                    unfinishedQueryBuilder.must(matchQuery("status.keyword",
                        Status.INCOMPLETE.toString()));
                    unfinishedQueryBuilder.must(nestedQuery("organizations",
                        termsQuery("organizations.id.keyword",
                            userOrganizationIds),
                        ScoreMode.None));
                    boolQueryBuilder.should(unfinishedQueryBuilder);
                }
                boolQueryBuilder.minimumShouldMatch(1);
                builder.must(boolQueryBuilder);
            }
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta,
                response);
            response.getHits().forEach(hit -> {
                try {
                    codeSchemes.add(mapper.readValue(hit.getSourceAsString(),
                        CodeSchemeDTO.class));
                } catch (final IOException e) {
                    LOG.error("getCodeSchemes reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            });
        }
        return codeSchemes;
    }

    private List<String> getRegularStatuses() {
        final List<String> allStatuses = new ArrayList<>();
        allStatuses.add(Status.DRAFT.toString());
        allStatuses.add(Status.SUGGESTED.toString());
        allStatuses.add(Status.SUBMITTED.toString());
        allStatuses.add(Status.VALID.toString());
        allStatuses.add(Status.INVALID.toString());
        allStatuses.add(Status.RETIRED.toString());
        allStatuses.add(Status.SUPERSEDED.toString());
        return allStatuses;
    }

    public CodeDTO getCode(final String codeRegistryCodeValue,
                           final String codeSchemeCodeValue,
                           final String codeCodeValue) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_CODE).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODE)
                .setTypes(ELASTIC_TYPE_CODE);
            final BoolQueryBuilder builder = boolQuery()
                .should(matchQuery("id",
                    codeCodeValue.toLowerCase()))
                .should(matchQuery("codeValue",
                    codeCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER))
                .minimumShouldMatch(1);
            builder.must(boolQuery()
                .should(matchQuery("codeScheme.id",
                    codeSchemeCodeValue.toLowerCase()))
                .should(matchQuery("codeScheme.codeValue",
                    codeSchemeCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER))
                .minimumShouldMatch(1));
            builder.must(matchQuery("codeScheme.codeRegistry.codeValue",
                codeRegistryCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER));
            searchRequest.setQuery(builder);

            final SearchResponse response = searchRequest.execute().actionGet();
            LOG.debug(String.format("getCode found: %d hits.",
                response.getHits().getTotalHits()));
            if (response.getHits().getTotalHits() > 0) {
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(),
                            CodeDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getCode reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            }
            return null;
        } else {
            return null;
        }
    }

    public Set<CodeDTO> getCodesByCodeRegistryCodeValueAndCodeSchemeCodeValue(final String codeRegistryCodeValue,
                                                                              final String codeSchemeCodeValue) {
        return getCodes(MAX_SIZE,
            0,
            codeRegistryCodeValue,
            codeSchemeCodeValue,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    }

    public Set<CodeDTO> getCodes(final Integer pageSize,
                                 final Integer from,
                                 final String codeRegistryCodeValue,
                                 final String codeSchemeCodeValue,
                                 final String codeCodeValue,
                                 final String prefLabel,
                                 final Integer hierarchyLevel,
                                 final String broaderCodeId,
                                 final String language,
                                 final List<String> statuses,
                                 final Date after,
                                 final Meta meta) {
        validatePageSize(pageSize);
        final Set<CodeDTO> codes = new LinkedHashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_CODE).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODE)
                .setTypes(ELASTIC_TYPE_CODE)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0);

            final BoolQueryBuilder builder = constructSearchQuery(codeCodeValue,
                prefLabel,
                after);
            builder.must(matchQuery("codeScheme.codeRegistry.codeValue",
                codeRegistryCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER));
            builder.must(boolQuery()
                .should(matchQuery("codeScheme.codeValue",
                    codeSchemeCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER))
                .should(matchQuery("codeScheme.id",
                    codeSchemeCodeValue.toLowerCase()))
                .minimumShouldMatch(1));
            searchRequest.setQuery(builder);
            if (hierarchyLevel != null) {
                builder.must(rangeQuery("hierarchyLevel").lte(hierarchyLevel));
            }
            if (broaderCodeId != null && !broaderCodeId.isEmpty()) {
                builder.must(matchQuery("broaderCode.id",
                    broaderCodeId.toLowerCase()));
            }
            if (statuses != null && !statuses.isEmpty()) {
                builder.must(termsQuery("status.keyword",
                    statuses));
            }
            if (language != null && !language.isEmpty()) {
                searchRequest.addSort(SortBuilders.fieldSort("prefLabel." + language + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                sortLanguages.forEach(sortLanguage -> {
                    if (!language.equalsIgnoreCase(sortLanguage)) {
                        searchRequest.addSort(SortBuilders.fieldSort("prefLabel." + sortLanguage + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                    }
                });
                searchRequest.addSort("codeValue.raw",
                    SortOrder.ASC);
            } else {
                searchRequest.addSort("order",
                    SortOrder.ASC);
            }
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta,
                response);
            response.getHits().forEach(hit -> {
                try {
                    codes.add(mapper.readValue(hit.getSourceAsString(),
                        CodeDTO.class));
                } catch (final IOException e) {
                    LOG.error("getCodes reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            });
            return codes;
        }
        return codes;
    }

    public PropertyTypeDTO getPropertyType(final String propertyTypeIdentifier) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_PROPERTYTYPE).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_PROPERTYTYPE)
                .setTypes(ELASTIC_TYPE_PROPERTYTYPE);
            final BoolQueryBuilder builder = new BoolQueryBuilder()
                .should(matchQuery("id",
                    propertyTypeIdentifier.toLowerCase()))
                .should(matchQuery("localName",
                    propertyTypeIdentifier.toLowerCase()).analyzer(TEXT_ANALYZER))
                .minimumShouldMatch(1);
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            if (response.getHits().getTotalHits() > 0) {
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(),
                            PropertyTypeDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getPropertyType reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            }
        }
        return null;
    }

    public Set<PropertyTypeDTO> getPropertyTypes(final Integer pageSize,
                                                 final Integer from,
                                                 final String propertyTypePrefLabel,
                                                 final String context,
                                                 final String language,
                                                 final String type,
                                                 final Date after,
                                                 final Meta meta) {
        validatePageSize(pageSize);
        final Set<PropertyTypeDTO> propertyTypes = new LinkedHashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_PROPERTYTYPE).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_PROPERTYTYPE)
                .setTypes(ELASTIC_TYPE_PROPERTYTYPE)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0);
            final BoolQueryBuilder builder = constructSearchQuery(null,
                propertyTypePrefLabel,
                after);
            if (context != null) {
                builder.must(prefixQuery("context",
                    context.toLowerCase()));
            }
            if (type != null) {
                builder.must(prefixQuery("type",
                    type.toLowerCase()));
            }
            if (language != null && !language.isEmpty()) {
                searchRequest.addSort(SortBuilders.fieldSort("prefLabel." + language + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                sortLanguages.forEach(sortLanguage -> {
                    if (!language.equalsIgnoreCase(sortLanguage)) {
                        searchRequest.addSort(SortBuilders.fieldSort("prefLabel." + sortLanguage + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                    }
                });
                searchRequest.addSort("localName.keyword",
                    SortOrder.ASC);
            } else {
                searchRequest.addSort("localName.keyword",
                    SortOrder.ASC);
            }
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta,
                response);
            response.getHits().forEach(hit -> {
                try {
                    final PropertyTypeDTO propertyType = mapper.readValue(hit.getSourceAsString(),
                        PropertyTypeDTO.class);
                    propertyTypes.add(propertyType);
                } catch (final IOException e) {
                    LOG.error("getPropertyTypes reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            });
        }
        return propertyTypes;
    }

    public ValueTypeDTO getValueType(final String valueTypeIdentifier) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_VALUETYPE).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_VALUETYPE)
                .setTypes(ELASTIC_TYPE_VALUETYPE);
            final BoolQueryBuilder builder = new BoolQueryBuilder()
                .should(matchQuery("id",
                    valueTypeIdentifier.toLowerCase()))
                .should(matchQuery("localName",
                    valueTypeIdentifier.toLowerCase()).analyzer(TEXT_ANALYZER))
                .minimumShouldMatch(1);
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            if (response.getHits().getTotalHits() > 0) {
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(),
                            ValueTypeDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getValueType reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            }
        }
        return null;
    }

    public Set<ValueTypeDTO> getValueTypes(final Integer pageSize,
                                           final Integer from,
                                           final String localName,
                                           final Date after,
                                           final Meta meta) {
        validatePageSize(pageSize);
        final Set<ValueTypeDTO> valueTypes = new LinkedHashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_VALUETYPE).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_VALUETYPE)
                .setTypes(ELASTIC_TYPE_VALUETYPE)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0);
            final BoolQueryBuilder builder = constructSearchQuery(null,
                null,
                after);
            if (localName != null) {
                builder.must(prefixQuery("localName",
                    localName.toLowerCase()));
            }
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta,
                response);
            response.getHits().forEach(hit -> {
                try {
                    final ValueTypeDTO valueType = mapper.readValue(hit.getSourceAsString(),
                        ValueTypeDTO.class);
                    valueTypes.add(valueType);
                } catch (final IOException e) {
                    LOG.error("getValueTypes reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            });
        }
        return valueTypes;
    }

    public ExternalReferenceDTO getExternalReference(final String externalReferenceId) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_EXTERNALREFERENCE).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_EXTERNALREFERENCE)
                .setTypes(ELASTIC_TYPE_EXTERNALREFERENCE);
            final BoolQueryBuilder builder = boolQuery()
                .must(matchQuery("id",
                    externalReferenceId.toLowerCase()));
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            if (response.getHits().getTotalHits() > 0) {
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(),
                            ExternalReferenceDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getExternalReference reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            }
        }
        return null;
    }

    public Set<ExternalReferenceDTO> getExternalReferences(final CodeSchemeDTO codeScheme) {
        return getExternalReferences(null,
            null,
            null,
            codeScheme,
            false,
            null,
            null);
    }

    public Set<ExternalReferenceDTO> getExternalReferences(final Integer pageSize,
                                                           final Integer from,
                                                           final String externalReferencePrefLabel,
                                                           final CodeSchemeDTO codeScheme,
                                                           final Boolean full,
                                                           final Date after,
                                                           final Meta meta) {
        validatePageSize(pageSize);
        final Set<ExternalReferenceDTO> externalReferences = new LinkedHashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_EXTERNALREFERENCE).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_EXTERNALREFERENCE)
                .setTypes(ELASTIC_TYPE_EXTERNALREFERENCE)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0);
            final BoolQueryBuilder builder = constructSearchQuery(null,
                externalReferencePrefLabel,
                after);
            searchRequest.setQuery(builder);
            if (codeScheme != null) {
                builder.should(boolQuery()
                    .should(boolQuery()
                        .must(matchQuery("parentCodeScheme.codeRegistry.codeValue",
                            codeScheme.getCodeRegistry().getCodeValue().toLowerCase()).analyzer(TEXT_ANALYZER))
                        .must(matchQuery("parentCodeScheme.id",
                            codeScheme.getId().toString().toLowerCase())))
                    .should(boolQuery()
                        .must(matchQuery("global",
                            true))));
            } else if (!full) {
                builder.must(matchQuery("global",
                    true));
            }
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta,
                response);
            response.getHits().forEach(hit -> {
                try {
                    final ExternalReferenceDTO externalReference = mapper.readValue(hit.getSourceAsString(),
                        ExternalReferenceDTO.class);
                    externalReferences.add(externalReference);
                } catch (final IOException e) {
                    LOG.error("getExternalReferences reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            });
        }
        return externalReferences;
    }

    public Set<ExtensionDTO> getExtensions(final Integer pageSize,
                                           final Integer from,
                                           final String extensionPrefLabel,
                                           final CodeSchemeDTO codeScheme,
                                           final Date after,
                                           final Meta meta) {
        validatePageSize(pageSize);
        final Set<ExtensionDTO> extensions = new LinkedHashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_EXTENSION).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_EXTENSION)
                .setTypes(ELASTIC_TYPE_EXTENSION)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0)
                .addSort("codeValue.raw",
                    SortOrder.ASC);
            final BoolQueryBuilder builder = constructSearchQuery(null,
                extensionPrefLabel,
                after);
            searchRequest.setQuery(builder);
            if (codeScheme != null) {
                builder.must(matchQuery("parentCodeScheme.id",
                    codeScheme.getId().toString().toLowerCase()));
            }
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta,
                response);
            response.getHits().forEach(hit -> {
                try {
                    final ExtensionDTO extension = mapper.readValue(hit.getSourceAsString(),
                        ExtensionDTO.class);
                    extensions.add(extension);
                } catch (final IOException e) {
                    LOG.error("getExtensions reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            });
        }
        return extensions;
    }

    public ExtensionDTO getExtension(final UUID codeSchemeUuid,
                                     final String extensionCodeValue) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_EXTENSION).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_EXTENSION)
                .setTypes(ELASTIC_TYPE_EXTENSION)
                .addSort("codeValue.raw",
                    SortOrder.ASC);
            final BoolQueryBuilder builder = boolQuery()
                .should(matchQuery("id",
                    extensionCodeValue.toLowerCase()))
                .should(matchQuery("codeValue",
                    extensionCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER))
                .minimumShouldMatch(1);
            builder.must(matchQuery("parentCodeScheme.id",
                codeSchemeUuid.toString().toLowerCase()));
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            if (response.getHits().getTotalHits() > 0) {
                LOG.debug(String.format("Found %d Extensions",
                    response.getHits().getTotalHits()));
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(),
                            ExtensionDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getExtension reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            }
        }
        return null;
    }

    public ExtensionDTO getExtension(final String codeRegistryCodeValue,
                                     final String codeSchemeCodeValue,
                                     final String extensionCodeValue) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_EXTENSION).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_EXTENSION)
                .setTypes(ELASTIC_TYPE_EXTENSION)
                .addSort("codeValue.raw",
                    SortOrder.ASC);
            final BoolQueryBuilder builder = boolQuery()
                .should(matchQuery("id",
                    extensionCodeValue.toLowerCase()))
                .should(matchQuery("codeValue",
                    extensionCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER))
                .minimumShouldMatch(1);
            builder.must(matchQuery("parentCodeScheme.codeRegistry.codeValue",
                codeRegistryCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER));
            builder.must(matchQuery("parentCodeScheme.codeValue",
                codeSchemeCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER));
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            if (response.getHits().getTotalHits() > 0) {
                LOG.debug(String.format("Found %d Extensions",
                    response.getHits().getTotalHits()));
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(),
                            ExtensionDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getExtension reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            }
        }
        return null;
    }

    public ExtensionDTO getExtension(final String extensionId) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_EXTENSION).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_EXTENSION)
                .setTypes(ELASTIC_TYPE_EXTENSION);
            final BoolQueryBuilder builder = boolQuery()
                .must(matchQuery("id",
                    extensionId.toLowerCase()));
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            if (response.getHits().getTotalHits() > 0) {
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(),
                            ExtensionDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getExtension reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            }
        }
        return null;
    }

    public Set<MemberDTO> getMembers(final Integer pageSize,
                                     final Integer from,
                                     final CodeDTO code,
                                     final Date after,
                                     final Meta meta) {
        validatePageSize(pageSize);
        final Set<MemberDTO> members = new LinkedHashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_MEMBER).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_MEMBER)
                .setTypes(ELASTIC_TYPE_MEMBER)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0)
                .addSort("order",
                    SortOrder.ASC);
            final BoolQueryBuilder builder = constructSearchQuery(null,
                null,
                after);
            searchRequest.setQuery(builder);
            if (code != null) {
                builder.must(matchQuery("code.id",
                    code.getId().toString().toLowerCase()));
            }
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta,
                response);
            response.getHits().forEach(hit -> {
                try {
                    final MemberDTO member = mapper.readValue(hit.getSourceAsString(),
                        MemberDTO.class);
                    members.add(member);
                } catch (final IOException e) {
                    LOG.error("getMembers reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            });
        }
        return members;
    }

    public Set<MemberDTO> getMembers(final Integer pageSize,
                                     final Integer from,
                                     final Date after,
                                     final Meta meta) {
        validatePageSize(pageSize);
        final Set<MemberDTO> members = new LinkedHashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_MEMBER).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_MEMBER)
                .setTypes(ELASTIC_TYPE_MEMBER)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0)
                .addSort("order",
                    SortOrder.ASC);
            final BoolQueryBuilder builder = constructSearchQuery(null,
                null,
                after);
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta,
                response);
            response.getHits().forEach(hit -> {
                try {
                    final MemberDTO member = mapper.readValue(hit.getSourceAsString(),
                        MemberDTO.class);
                    members.add(member);
                } catch (final IOException e) {
                    LOG.error("getMembers reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                }
            });
        }
        return members;
    }

    public Set<MemberDTO> getMembers(final Integer pageSize,
                                     final Integer from,
                                     final ExtensionDTO extension,
                                     final Date after,
                                     final Meta meta) {
        validatePageSize(pageSize);
        final Set<MemberDTO> members = new LinkedHashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_MEMBER).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_MEMBER)
                .setTypes(ELASTIC_TYPE_MEMBER)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0)
                .addSort("order",
                    SortOrder.ASC);
            final BoolQueryBuilder builder = constructSearchQuery(null,
                null,
                after);
            searchRequest.setQuery(builder);
            if (extension != null) {
                builder.must(matchQuery("extension.id",
                    extension.getId().toString().toLowerCase()));
            }
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta,
                response);
            response.getHits().forEach(hit -> {
                try {
                    final MemberDTO member = mapper.readValue(hit.getSourceAsString(),
                        MemberDTO.class);
                    members.add(member);
                } catch (final IOException e) {
                    LOG.error("getMembers reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                }
            });
        }
        return members;
    }

    public MemberDTO getMember(final String memberId) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_MEMBER).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_MEMBER)
                .setTypes(ELASTIC_TYPE_MEMBER);
            final BoolQueryBuilder builder = boolQuery()
                .must(matchQuery("id",
                    memberId.toLowerCase()));
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            if (response.getHits().getTotalHits() > 0) {
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(),
                            MemberDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getMember reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                }
            }
        }
        return null;
    }

    public Set<ResourceDTO> getContainers(final Integer pageSize,
                                          final Integer from,
                                          final String language,
                                          final List<String> statuses,
                                          final Date after,
                                          final Meta meta) {
        validatePageSize(pageSize);
        final Set<ResourceDTO> containers = new LinkedHashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_CODESCHEME).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODESCHEME)
                .setTypes(ELASTIC_TYPE_CODESCHEME)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0);
            final BoolQueryBuilder builder = boolQuery();
            if (after != null) {
                final ISO8601DateFormat dateFormat = new ISO8601DateFormat();
                final String afterString = dateFormat.format(after);
                builder.must(rangeQuery("modified").gt(afterString));
            }
            if (language != null && !language.isEmpty()) {
                searchRequest.addSort(SortBuilders.fieldSort("prefLabel." + language + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                sortLanguages.forEach(sortLanguage -> {
                    if (!language.equalsIgnoreCase(sortLanguage)) {
                        searchRequest.addSort(SortBuilders.fieldSort("prefLabel." + sortLanguage + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                    }
                });
                searchRequest.addSort("codeValue.raw",
                    SortOrder.ASC);
            } else {
                searchRequest.addSort("codeValue.raw",
                    SortOrder.ASC);
            }
            if (statuses != null && !statuses.isEmpty()) {
                final BoolQueryBuilder boolQueryBuilder = boolQuery();
                if (statuses.contains(Status.INCOMPLETE.toString())) {
                    final BoolQueryBuilder unfinishedQueryBuilder = boolQuery();
                    unfinishedQueryBuilder.must(matchQuery("status.keyword",
                        Status.INCOMPLETE.toString()));
                    boolQueryBuilder.should(unfinishedQueryBuilder);
                    statuses.remove(Status.INCOMPLETE.toString());
                }
                boolQueryBuilder.should(termsQuery("status.keyword",
                    statuses));
                builder.must(termsQuery("status.keyword",
                    statuses));
                boolQueryBuilder.minimumShouldMatch(1);
                builder.must(boolQueryBuilder);
            } else {
                final BoolQueryBuilder boolQueryBuilder = boolQuery();
                boolQueryBuilder.should(termsQuery("status.keyword",
                    getRegularStatuses()));
                boolQueryBuilder.minimumShouldMatch(1);
                builder.must(boolQueryBuilder);
            }
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta,
                response);
            response.getHits().forEach(hit -> {
                try {
                    containers.add(mapper.readValue(hit.getSourceAsString(),
                        ResourceDTO.class));
                } catch (final IOException e) {
                    LOG.error("getContainers reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            });
        }
        return containers;
    }

    public Set<ResourceDTO> getResources(final Integer pageSize,
                                         final Integer from,
                                         final String codeSchemeUri,
                                         final String language,
                                         final List<String> statuses,
                                         final Date after,
                                         final Meta meta) {
        validatePageSize(pageSize);
        final Set<ResourceDTO> resources = new LinkedHashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_CODE).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODE)
                .setTypes(ELASTIC_TYPE_CODE)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0);

            final BoolQueryBuilder builder = constructSearchQuery(null,
                null,
                after);
            builder.must(matchQuery("codeScheme.uri.keyword",
                codeSchemeUri.toLowerCase()).analyzer(TEXT_ANALYZER));
            if (statuses != null && !statuses.isEmpty()) {
                builder.must(termsQuery("status.keyword",
                    statuses));
            }
            if (language != null && !language.isEmpty()) {
                searchRequest.addSort(SortBuilders.fieldSort("prefLabel." + language + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                sortLanguages.forEach(sortLanguage -> {
                    if (!language.equalsIgnoreCase(sortLanguage)) {
                        searchRequest.addSort(SortBuilders.fieldSort("prefLabel." + sortLanguage + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                    }
                });
                searchRequest.addSort("codeValue.raw",
                    SortOrder.ASC);
            } else {
                searchRequest.addSort("order",
                    SortOrder.ASC);
            }
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta,
                response);
            response.getHits().forEach(hit -> {
                try {
                    resources.add(mapper.readValue(hit.getSourceAsString(),
                        ResourceDTO.class));
                } catch (final IOException e) {
                    LOG.error("getResources reading value from JSON string failed: " + hit.getSourceAsString(),
                        e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            });
            return resources;
        }
        return resources;
    }

    private BoolQueryBuilder constructSearchQuery(final String codeValue,
                                                  final String prefLabel,
                                                  final Date after) {
        final BoolQueryBuilder builder = boolQuery();
        if (codeValue != null) {
            builder.must(prefixQuery("codeValue",
                codeValue.toLowerCase()));
        }
        if (prefLabel != null) {
            builder.must(nestedQuery("prefLabel",
                multiMatchQuery(prefLabel.toLowerCase(),
                    "prefLabel.*").type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX),
                ScoreMode.None));
        }
        if (after != null) {
            final ISO8601DateFormat dateFormat = new ISO8601DateFormat();
            final String afterString = dateFormat.format(after);
            builder.must(rangeQuery("modified").gt(afterString));
        }
        return builder;
    }

    private void setResultCounts(final Meta meta,
                                 final SearchResponse response) {
        final Integer totalResults = toIntExact(response.getHits().totalHits);
        final Integer resultCount = toIntExact(response.getHits().getHits().length);
        if (meta != null) {
            meta.setTotalResults(totalResults);
            meta.setResultCount(resultCount);
        }
        LOG.debug(String.format("Search found: %d total hits.",
            totalResults));
    }

    private void boostStatus(final BoolQueryBuilder builder) {
        builder.should(constantScoreQuery(termQuery("status.keyword",
            "VALID")).boost(1000f));
        builder.should(constantScoreQuery(termQuery("status.keyword",
            "SUBMITTED")).boost(900f));
        builder.should(constantScoreQuery(termQuery("status.keyword",
            "DRAFT")).boost(800f));
        builder.should(constantScoreQuery(termQuery("status.keyword",
            "SUGGESTED")).boost(700f));
        builder.should(constantScoreQuery(termQuery("status.keyword",
            "SUPERSEDED")).boost(600f));
        builder.should(constantScoreQuery(termQuery("status.keyword",
            "RETIRED")).boost(500f));
        builder.should(constantScoreQuery(termQuery("status.keyword",
            "INVALID")).boost(400f));
        builder.should(constantScoreQuery(termQuery("status.keyword",
            "INCOMPLETE")).boost(300f));
    }

    private void validatePageSize(final Integer pageSize) {
        if (pageSize != null && pageSize > MAX_SIZE) {
            throw new YtiCodeListException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(),
                String.format("Paging pageSize parameter value %d exceeds max value %d.",
                    pageSize,
                    MAX_SIZE)));
        }
    }
}
