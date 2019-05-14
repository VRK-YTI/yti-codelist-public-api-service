package fi.vm.yti.codelist.api.domain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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
import fi.vm.yti.codelist.common.dto.SearchHitDTO;
import fi.vm.yti.codelist.common.dto.SearchResultWithMetaDataDTO;
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
    private static final int MAX_DEEP_SEARCH_SIZE = 3100; // FOR SOME REASON 31 RETURNS 30 RESULTS, 10 RETURNS 9 ETC.
    private static final String TEXT_ANALYZER = "text_analyzer";
    private static final String BOOSTSTATUS = "boostStatus";
    private static final Set<String> sortLanguages = new HashSet<>(Arrays.asList(LANGUAGE_CODE_FI,
        LANGUAGE_CODE_EN,
        LANGUAGE_CODE_SV));
    private final RestHighLevelClient client;

    @Inject
    private DomainImpl(final RestHighLevelClient client) {
        this.client = client;
    }

    private void registerModulesToMapper(ObjectMapper mapper) {
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,
            false);
    }

    private boolean checkIfIndexExists(final String indexName) {
        final GetIndexRequest request = new GetIndexRequest();
        request.indices(indexName);
        try {
            return client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (final IOException e) {
            LOG.error("Index checking request failed for index: " + indexName, e);
            throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
        }
    }

    public CodeRegistryDTO getCodeRegistry(final String codeRegistryCodeValue) {
        if (checkIfIndexExists(ELASTIC_INDEX_CODEREGISTRY)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_CODEREGISTRY);
            searchRequest.types(ELASTIC_TYPE_CODEREGISTRY);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.sort("codeValue.raw", SortOrder.ASC);
            final BoolQueryBuilder builder = boolQuery()
                .should(matchQuery("id",
                    codeRegistryCodeValue.toLowerCase()))
                .should(matchQuery("codeValue",
                    codeRegistryCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER))
                .minimumShouldMatch(1);
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
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
        if (checkIfIndexExists(ELASTIC_INDEX_CODEREGISTRY)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_CODEREGISTRY);
            searchRequest.types(ELASTIC_TYPE_CODEREGISTRY);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.sort("codeValue.raw", SortOrder.ASC);
            searchBuilder.size(pageSize != null ? pageSize : MAX_SIZE);
            searchBuilder.from(from != null ? from : 0);
            final BoolQueryBuilder builder = constructSearchQuery(codeRegistryCodeValue,
                codeRegistryPrefLabel,
                after);
            if (organizations != null && !organizations.isEmpty()) {
                builder.must(termsQuery("organizations.id.keyword",
                    organizations));
            }
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }
        }
        return codeRegistries;
    }

    public CodeSchemeDTO getCodeScheme(final String codeSchemeId) {
        if (checkIfIndexExists(ELASTIC_INDEX_CODESCHEME)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_CODESCHEME);
            searchRequest.types(ELASTIC_TYPE_CODESCHEME);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.sort("codeValue.raw", SortOrder.ASC);
            final BoolQueryBuilder builder = boolQuery()
                .must(matchQuery("id",
                    codeSchemeId.toLowerCase()));
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }
        }
        return null;
    }

    public CodeSchemeDTO getCodeScheme(final String codeRegistryCodeValue,
                                       final String codeSchemeCodeValue) {
        if (checkIfIndexExists(ELASTIC_INDEX_CODESCHEME)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_CODESCHEME);
            searchRequest.types(ELASTIC_TYPE_CODESCHEME);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.sort("codeValue.raw", SortOrder.ASC);
            final BoolQueryBuilder builder = boolQuery()
                .should(matchQuery("id",
                    codeSchemeCodeValue.toLowerCase()))
                .should(matchQuery("codeValue",
                    codeSchemeCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER))
                .minimumShouldMatch(1);
            builder.must(matchQuery("codeRegistry.codeValue",
                codeRegistryCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER));
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
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

    private SearchResultWithMetaDataDTO getCodeSchemesMatchingCodes(final String searchTerm,
                                                                    final SearchResultWithMetaDataDTO result) {
        final Set<String> codeSchemeUuids = new HashSet<>();
        if (checkIfIndexExists(ELASTIC_INDEX_CODE)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_CODE);
            searchRequest.types(ELASTIC_TYPE_CODE);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.sort("codeValue.raw", SortOrder.ASC);
            searchBuilder.size(MAX_DEEP_SEARCH_SIZE);
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
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                response.getHits().forEach(hit -> {
                    try {
                        CodeDTO codeDTO = mapper.readValue(hit.getSourceAsString(),
                            CodeDTO.class);
                        String uuidOfTheCodeScheme = codeDTO.getCodeScheme().getId().toString().toLowerCase();
                        populateSearchHits(codeSchemeUuids,
                            result,
                            codeDTO.getPrefLabel(),
                            codeDTO.getUri(),
                            codeDTO.getCodeValue(),
                            codeDTO.getCodeScheme().getCodeValue(),
                            codeDTO.getCodeScheme().getCodeRegistry().getCodeValue(),
                            uuidOfTheCodeScheme,
                            SEARCH_HIT_TYPE_CODE);
                    } catch (final IOException e) {
                        LOG.error("getCodeSchemesMatchingCodes reading value from JSON string failed: " + hit.getSourceAsString(),
                            e);
                        throw new JsonParsingException(ERR_MSG_USER_406);
                    }
                });
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }
            result.getResults().addAll(codeSchemeUuids);
            return result;
        }
        return result;
    }

    private SearchResultWithMetaDataDTO getCodeSchemesMatchingExtensions(final String searchTerm,
                                                                         final String extensionPropertyType,
                                                                         final SearchResultWithMetaDataDTO result) {
        final Set<String> codeSchemeUuids = new HashSet<>();
        if (checkIfIndexExists(ELASTIC_INDEX_EXTENSION)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_CODE);
            searchRequest.types(ELASTIC_TYPE_CODE);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.size(MAX_DEEP_SEARCH_SIZE);
            final BoolQueryBuilder builder = boolQuery();
            if (searchTerm != null) {
                final BoolQueryBuilder boolQueryBuilder = boolQuery();
                if (searchTerm != null && !searchTerm.isEmpty()) {
                    boolQueryBuilder.should(prefixQuery("codeValue",
                        searchTerm.toLowerCase()));
                    boolQueryBuilder.should(nestedQuery("prefLabel",
                        multiMatchQuery(searchTerm.toLowerCase() + "*",
                            "prefLabel.*").type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX),
                        ScoreMode.None));
                }
                boolQueryBuilder.minimumShouldMatch(1);
                builder.must(boolQueryBuilder);
            }
            if (extensionPropertyType != null) {
                builder.must(matchQuery("propertyType.localName",
                    extensionPropertyType));
            }
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                response.getHits().forEach(hit -> {
                    try {
                        ExtensionDTO extensionDTO = mapper.readValue(hit.getSourceAsString(),
                            ExtensionDTO.class);
                        String uuidOfTheCodeScheme = extensionDTO.getParentCodeScheme().getId().toString().toLowerCase();
                        codeSchemeUuids.add(uuidOfTheCodeScheme);
                        populateSearchHits(codeSchemeUuids,
                            result,
                            extensionDTO.getPrefLabel(),
                            extensionDTO.getUri(),
                            extensionDTO.getCodeValue(),
                            extensionDTO.getParentCodeScheme().getCodeValue(),
                            extensionDTO.getParentCodeScheme().getCodeRegistry().getCodeValue(),
                            uuidOfTheCodeScheme,
                            SEARCH_HIT_TYPE_EXTENSION);
                    } catch (final IOException e) {
                        LOG.error("getCodeSchemesMatchingExtensions reading value from JSON string failed: " + hit.getSourceAsString(),
                            e);
                        throw new JsonParsingException(ERR_MSG_USER_406);
                    }
                });
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }
            result.getResults().addAll(codeSchemeUuids);
            return result;
        }
        return result;
    }

    private void populateSearchHits(final Set<String> codeSchemeUuids,
                                    final SearchResultWithMetaDataDTO result,
                                    final Map<String, String> prefLabel,
                                    final String uri,
                                    final String entityCodeValue,
                                    final String codeSchemeCodeValue,
                                    final String codeRegistryCodeValue,
                                    final String uuidOfTheCodeScheme,
                                    final String typeOfHit) {
        codeSchemeUuids.add(uuidOfTheCodeScheme);
        SearchHitDTO searchHit = new SearchHitDTO();
        searchHit.setType(typeOfHit);
        searchHit.setPrefLabel(prefLabel);
        searchHit.setUri(uri);
        searchHit.setEntityCodeValue(entityCodeValue);
        searchHit.setCodeSchemeCodeValue(codeSchemeCodeValue);
        searchHit.setCodeRegistryCodeValue(codeRegistryCodeValue);

        Map<String, ArrayList<SearchHitDTO>> searchHits = result.getSearchHitDTOMap();
        if (searchHits.containsKey(uuidOfTheCodeScheme)) {
            ArrayList<SearchHitDTO> searchHitList = searchHits.get(uuidOfTheCodeScheme);
            searchHitList.add(searchHit);
            searchHits.put(uuidOfTheCodeScheme,
                searchHitList);
        } else {
            ArrayList<SearchHitDTO> searchHitList = new ArrayList<>();
            searchHitList.add(searchHit);
            searchHits.put(uuidOfTheCodeScheme,
                searchHitList);
        }
        result.getSearchHitDTOMap().put(uuidOfTheCodeScheme,
            searchHits.get(uuidOfTheCodeScheme));
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
        SearchResultWithMetaDataDTO searchResultWithMetaData = new SearchResultWithMetaDataDTO();

        if (searchExtensions && searchTerm != null) {
            searchResultWithMetaData = getCodeSchemesMatchingExtensions(searchTerm,
                extensionPropertyType,
                searchResultWithMetaData);
            codeSchemeUuids.addAll(searchResultWithMetaData.getResults());
        }

        if (searchCodes && searchTerm != null) {
            searchResultWithMetaData = getCodeSchemesMatchingCodes(searchTerm,
                searchResultWithMetaData);
            codeSchemeUuids.addAll(searchResultWithMetaData.getResults());
        }

        final Set<CodeSchemeDTO> codeSchemes = new LinkedHashSet<>();
        if (checkIfIndexExists(ELASTIC_INDEX_CODESCHEME)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_CODESCHEME);
            searchRequest.types(ELASTIC_TYPE_CODESCHEME);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.size(pageSize != null ? pageSize : MAX_SIZE);
            searchBuilder.from(from != null ? from : 0);
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
                searchBuilder.sort(SortBuilders.scoreSort());
                boostStatus(builder);
            }
            if (language != null && !language.isEmpty()) {
                searchBuilder.sort(SortBuilders.fieldSort("prefLabel." + language + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                sortLanguages.forEach(sortLanguage -> {
                    if (!language.equalsIgnoreCase(sortLanguage)) {
                        searchBuilder.sort(SortBuilders.fieldSort("prefLabel." + sortLanguage + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                    }
                });
                searchBuilder.sort("codeValue.raw", SortOrder.ASC);
            } else {
                searchBuilder.sort("codeValue.raw", SortOrder.ASC);
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
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }
        }

        for (CodeSchemeDTO cs : codeSchemes) {
            ArrayList<SearchHitDTO> searchHits = searchResultWithMetaData.getSearchHitDTOMap().get(cs.getId().toString().toLowerCase());
            if (language != null && searchHits != null) {
                searchHits.sort(Comparator.comparing(searchHitDTO -> searchHitDTO.getPrefLabel().get(language) != null ? searchHitDTO.getPrefLabel().get(language) : searchHitDTO.getEntityCodeValue(), Comparator.nullsLast(Comparator.naturalOrder())));
            }
            cs.setSearchHits(searchHits);
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
        if (checkIfIndexExists(ELASTIC_INDEX_CODE)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_CODE);
            searchRequest.types(ELASTIC_TYPE_CODE);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
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
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
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
        if (checkIfIndexExists(ELASTIC_INDEX_CODE)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_CODE);
            searchRequest.types(ELASTIC_TYPE_CODE);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.size(pageSize != null ? pageSize : MAX_SIZE);
            searchBuilder.from(from != null ? from : 0);
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
                searchBuilder.sort(SortBuilders.fieldSort("prefLabel." + language + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                sortLanguages.forEach(sortLanguage -> {
                    if (!language.equalsIgnoreCase(sortLanguage)) {
                        searchBuilder.sort(SortBuilders.fieldSort("prefLabel." + sortLanguage + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                    }
                });
                searchBuilder.sort("codeValue.raw",
                    SortOrder.ASC);
            } else {
                searchBuilder.sort("order",
                    SortOrder.ASC);
            }
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }
            return codes;
        }
        return codes;
    }

    public PropertyTypeDTO getPropertyType(final String propertyTypeIdentifier) {
        if (checkIfIndexExists(ELASTIC_INDEX_PROPERTYTYPE)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_PROPERTYTYPE);
            searchRequest.types(ELASTIC_TYPE_PROPERTYTYPE);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            final BoolQueryBuilder builder = new BoolQueryBuilder()
                .should(matchQuery("id",
                    propertyTypeIdentifier.toLowerCase()))
                .should(matchQuery("localName",
                    propertyTypeIdentifier.toLowerCase()).analyzer(TEXT_ANALYZER))
                .minimumShouldMatch(1);
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
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
        if (checkIfIndexExists(ELASTIC_INDEX_PROPERTYTYPE)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_PROPERTYTYPE);
            searchRequest.types(ELASTIC_TYPE_PROPERTYTYPE);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.size(pageSize != null ? pageSize : MAX_SIZE);
            searchBuilder.from(from != null ? from : 0);
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
                searchBuilder.sort(SortBuilders.fieldSort("prefLabel." + language + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                sortLanguages.forEach(sortLanguage -> {
                    if (!language.equalsIgnoreCase(sortLanguage)) {
                        searchBuilder.sort(SortBuilders.fieldSort("prefLabel." + sortLanguage + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                    }
                });
                searchBuilder.sort("localName.keyword",
                    SortOrder.ASC);
            } else {
                searchBuilder.sort("localName.keyword",
                    SortOrder.ASC);
            }
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                setResultCounts(meta, response);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }
        }
        return propertyTypes;
    }

    public ValueTypeDTO getValueType(final String valueTypeIdentifier) {
        if (checkIfIndexExists(ELASTIC_INDEX_VALUETYPE)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_VALUETYPE);
            searchRequest.types(ELASTIC_TYPE_VALUETYPE);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            final BoolQueryBuilder builder = new BoolQueryBuilder()
                .should(matchQuery("id",
                    valueTypeIdentifier.toLowerCase()))
                .should(matchQuery("localName",
                    valueTypeIdentifier.toLowerCase()).analyzer(TEXT_ANALYZER))
                .minimumShouldMatch(1);
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
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
        if (checkIfIndexExists(ELASTIC_INDEX_VALUETYPE)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_VALUETYPE);
            searchRequest.types(ELASTIC_TYPE_VALUETYPE);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.size(pageSize != null ? pageSize : MAX_SIZE);
            searchBuilder.from(from != null ? from : 0);
            final BoolQueryBuilder builder = constructSearchQuery(null,
                null,
                after);
            if (localName != null) {
                builder.must(prefixQuery("localName",
                    localName.toLowerCase()));
            }
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                setResultCounts(meta, response);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }
        }
        return valueTypes;
    }

    public ExternalReferenceDTO getExternalReference(final String externalReferenceId) {
        if (checkIfIndexExists(ELASTIC_INDEX_EXTERNALREFERENCE)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_EXTERNALREFERENCE);
            searchRequest.types(ELASTIC_TYPE_EXTERNALREFERENCE);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            final BoolQueryBuilder builder = boolQuery()
                .must(matchQuery("id",
                    externalReferenceId.toLowerCase()));
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
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
        if (checkIfIndexExists(ELASTIC_INDEX_EXTERNALREFERENCE)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_EXTERNALREFERENCE);
            searchRequest.types(ELASTIC_TYPE_EXTERNALREFERENCE);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.size(pageSize != null ? pageSize : MAX_SIZE);
            searchBuilder.from(from != null ? from : 0);
            final BoolQueryBuilder builder = constructSearchQuery(null,
                externalReferencePrefLabel,
                after);
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
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }
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
        if (checkIfIndexExists(ELASTIC_INDEX_EXTENSION)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_EXTENSION);
            searchRequest.types(ELASTIC_TYPE_EXTENSION);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.size(pageSize != null ? pageSize : MAX_SIZE);
            searchBuilder.from(from != null ? from : 0);
            searchBuilder.sort("codeValue.raw", SortOrder.ASC);
            final BoolQueryBuilder builder = constructSearchQuery(null,
                extensionPrefLabel,
                after);
            if (codeScheme != null) {
                builder.must(matchQuery("parentCodeScheme.id",
                    codeScheme.getId().toString().toLowerCase()));
            }
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }
        }
        return extensions;
    }

    public ExtensionDTO getExtension(final UUID codeSchemeUuid,
                                     final String extensionCodeValue) {
        if (checkIfIndexExists(ELASTIC_INDEX_EXTENSION)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_EXTENSION);
            searchRequest.types(ELASTIC_TYPE_EXTENSION);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.sort("codeValue.raw", SortOrder.ASC);
            final BoolQueryBuilder builder = boolQuery()
                .should(matchQuery("id",
                    extensionCodeValue.toLowerCase()))
                .should(matchQuery("codeValue",
                    extensionCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER))
                .minimumShouldMatch(1);
            builder.must(matchQuery("parentCodeScheme.id",
                codeSchemeUuid.toString().toLowerCase()));
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }
        }
        return null;
    }

    public ExtensionDTO getExtension(final String codeRegistryCodeValue,
                                     final String codeSchemeCodeValue,
                                     final String extensionCodeValue) {
        if (checkIfIndexExists(ELASTIC_INDEX_EXTENSION)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_EXTENSION);
            searchRequest.types(ELASTIC_TYPE_EXTENSION);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.sort("codeValue.raw", SortOrder.ASC);
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
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }
        }
        return null;
    }

    public ExtensionDTO getExtension(final String extensionId) {
        if (checkIfIndexExists(ELASTIC_INDEX_EXTENSION)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_EXTENSION);
            searchRequest.types(ELASTIC_TYPE_EXTENSION);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            final BoolQueryBuilder builder = boolQuery()
                .must(matchQuery("id",
                    extensionId.toLowerCase()));
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
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
        if (checkIfIndexExists(ELASTIC_INDEX_MEMBER)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_MEMBER);
            searchRequest.types(ELASTIC_TYPE_MEMBER);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.size(pageSize != null ? pageSize : MAX_SIZE);
            searchBuilder.from(from != null ? from : 0);
            searchBuilder.sort("order", SortOrder.ASC);
            final BoolQueryBuilder builder = constructSearchQuery(null,
                null,
                after);
            if (code != null) {
                builder.must(matchQuery("code.id",
                    code.getId().toString().toLowerCase()));
            }
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }
        }
        return members;
    }

    public Set<MemberDTO> getMembers(final Integer pageSize,
                                     final Integer from,
                                     final Date after,
                                     final Meta meta) {
        validatePageSize(pageSize);
        final Set<MemberDTO> members = new LinkedHashSet<>();
        if (checkIfIndexExists(ELASTIC_INDEX_MEMBER)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_MEMBER);
            searchRequest.types(ELASTIC_TYPE_MEMBER);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.size(pageSize != null ? pageSize : MAX_SIZE);
            searchBuilder.from(from != null ? from : 0);
            searchBuilder.sort("order", SortOrder.ASC);
            final BoolQueryBuilder builder = constructSearchQuery(null,
                null,
                after);
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }
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
        if (checkIfIndexExists(ELASTIC_INDEX_MEMBER)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_MEMBER);
            searchRequest.types(ELASTIC_TYPE_MEMBER);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.size(pageSize != null ? pageSize : MAX_SIZE);
            searchBuilder.from(from != null ? from : 0);
            searchBuilder.sort("order", SortOrder.ASC);
            final BoolQueryBuilder builder = constructSearchQuery(null,
                null,
                after);
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            if (extension != null) {
                builder.must(matchQuery("extension.id",
                    extension.getId().toString().toLowerCase()));
            }
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }
        }
        return members;
    }

    public MemberDTO getMember(final String memberId,
                               final String extensionCodeValue) {
        boolean memberIdIsUUID = true;
        try {
            UUID theUuid = UUID.fromString(memberId);
        } catch (Exception e) {
            memberIdIsUUID = false;
        }
        if (checkIfIndexExists(ELASTIC_INDEX_MEMBER)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_MEMBER);
            searchRequest.types(ELASTIC_TYPE_MEMBER);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            if (memberIdIsUUID) {
                final BoolQueryBuilder builder = boolQuery()
                    .must(matchQuery("id",
                        memberId.toLowerCase()));
                searchBuilder.query(builder);
                searchRequest.source(searchBuilder);
            } else {
                final BoolQueryBuilder builder = boolQuery()
                    .must(matchQuery("sequenceId",
                        memberId))
                    .must(matchQuery("extension.codeValue",
                        extensionCodeValue));
                searchBuilder.query(builder);
                searchRequest.source(searchBuilder);
            }
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
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
        if (checkIfIndexExists(ELASTIC_INDEX_CODESCHEME)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_MEMBER);
            searchRequest.types(ELASTIC_TYPE_MEMBER);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.size(pageSize != null ? pageSize : MAX_SIZE);
            searchBuilder.from(from != null ? from : 0);
            final BoolQueryBuilder builder = boolQuery();
            if (after != null) {
                final ISO8601DateFormat dateFormat = new ISO8601DateFormat();
                final String afterString = dateFormat.format(after);
                builder.must(rangeQuery("modified").gt(afterString));
            }
            if (language != null && !language.isEmpty()) {
                searchBuilder.sort(SortBuilders.fieldSort("prefLabel." + language + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                sortLanguages.forEach(sortLanguage -> {
                    if (!language.equalsIgnoreCase(sortLanguage)) {
                        searchBuilder.sort(SortBuilders.fieldSort("prefLabel." + sortLanguage + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                    }
                });
                searchBuilder.sort("codeValue.raw", SortOrder.ASC);
            } else {
                searchBuilder.sort("codeValue.raw", SortOrder.ASC);
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
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }
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
        if (checkIfIndexExists(ELASTIC_INDEX_CODE)) {
            final ObjectMapper mapper = new ObjectMapper();
            registerModulesToMapper(mapper);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(ELASTIC_INDEX_CODE);
            searchRequest.types(ELASTIC_TYPE_CODE);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.size(pageSize != null ? pageSize : MAX_SIZE);
            searchBuilder.from(from != null ? from : 0);
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
                searchBuilder.sort(SortBuilders.fieldSort("prefLabel." + language + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                sortLanguages.forEach(sortLanguage -> {
                    if (!language.equalsIgnoreCase(sortLanguage)) {
                        searchBuilder.sort(SortBuilders.fieldSort("prefLabel." + sortLanguage + ".keyword").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("prefLabel")).unmappedType("keyword"));
                    }
                });
                searchBuilder.sort("codeValue.raw",
                    SortOrder.ASC);
            } else {
                searchBuilder.sort("order",
                    SortOrder.ASC);
            }
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "ElasticSearch index query error!"));
            }

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
