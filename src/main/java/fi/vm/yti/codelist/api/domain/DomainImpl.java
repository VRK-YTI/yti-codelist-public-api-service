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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import fi.vm.yti.codelist.api.dto.ResourceDTO;
import fi.vm.yti.codelist.api.exception.JsonParsingException;
import fi.vm.yti.codelist.api.exception.YtiCodeListException;
import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.CodeRegistryDTO;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.DeepSearchHitListDTO;
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
    private static final String TEXT_ANALYZER = "text_analyzer";
    private static final String BOOSTSTATUS = "boostStatus";
    private static final String ELASTIC_QUERY_ERROR = "ElasticSearch index query error!";
    private static final Set<String> sortLanguages = new HashSet<>(Arrays.asList(LANGUAGE_CODE_FI, LANGUAGE_CODE_EN, LANGUAGE_CODE_SV));
    private final RestHighLevelClient client;
    private final DeepCodeQueryFactory deepCodeQueryFactory;
    private final DeepExtensionQueryFactory deepExtensionQueryFactory;
    private final LuceneQueryFactory luceneQueryFactory;

    @Inject
    private DomainImpl(final RestHighLevelClient client) {
        this.client = client;
        this.luceneQueryFactory = new LuceneQueryFactory();
        this.deepCodeQueryFactory = new DeepCodeQueryFactory(new ObjectMapper(), this, luceneQueryFactory);
        this.deepExtensionQueryFactory = new DeepExtensionQueryFactory(new ObjectMapper(), this, luceneQueryFactory);
    }

    public CodeRegistryDTO getCodeRegistry(final String codeRegistryCodeValue) {
        if (checkIfIndexExists(ELASTIC_INDEX_CODEREGISTRY)) {
            final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
            final SearchRequest searchRequest = createSearchRequest(ELASTIC_INDEX_CODEREGISTRY);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.sort("codeValue.raw", SortOrder.ASC);
            final BoolQueryBuilder builder = boolQuery().should(matchQuery("id", codeRegistryCodeValue.toLowerCase())).should(matchQuery("codeValue", codeRegistryCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER)).minimumShouldMatch(1);
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                if (response.getHits().getTotalHits() > 0) {
                    final SearchHit hit = response.getHits().getAt(0);
                    LOG.debug(String.format("Found %d CodeRegistries", response.getHits().getTotalHits()));
                    try {
                        if (hit != null) {
                            return mapper.readValue(hit.getSourceAsString(), CodeRegistryDTO.class);
                        }
                    } catch (final IOException e) {
                        LOG.error("getCodeRegistry reading value from JSON string failed: " + hit.getSourceAsString(), e);
                        throw new JsonParsingException(ERR_MSG_USER_406);
                    }
                }
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
            }
        }
        return null;
    }

    public Set<CodeRegistryDTO> getCodeRegistries() {
        return getCodeRegistries(MAX_SIZE, 0, null, null, null, null, null);
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
            final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
            final SearchRequest searchRequest = createSearchRequest(ELASTIC_INDEX_CODEREGISTRY);
            final SearchSourceBuilder searchBuilder = createSearchSourceBuilderWithPagination(pageSize, from);
            searchBuilder.sort("codeValue.raw", SortOrder.ASC);
            final BoolQueryBuilder builder = constructSearchQuery(codeRegistryCodeValue, codeRegistryPrefLabel, after);
            if (organizations != null && !organizations.isEmpty()) {
                builder.must(termsQuery("organizations.id.keyword", organizations));
            }
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                setResultCounts(meta, response);
                response.getHits().forEach(hit -> {
                    LOG.debug(String.format("Found %d CodeRegistries", response.getHits().getTotalHits()));
                    try {
                        codeRegistries.add(mapper.readValue(hit.getSourceAsString(), CodeRegistryDTO.class));
                    } catch (final IOException e) {
                        LOG.error("getCodeRegistries reading value from JSON string failed: " + hit.getSourceAsString(), e);
                        throw new JsonParsingException(ERR_MSG_USER_406);
                    }
                });
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
            }
        }
        return codeRegistries;
    }

    public CodeSchemeDTO getCodeScheme(final String codeSchemeId) {
        if (checkIfIndexExists(ELASTIC_INDEX_CODESCHEME)) {
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.sort("codeValue.raw", SortOrder.ASC);
            final BoolQueryBuilder builder = boolQuery().must(matchQuery("id", codeSchemeId.toLowerCase()));
            searchBuilder.query(builder);
            return doCodeSchemeRequest(searchBuilder);
        }
        return null;
    }

    public CodeSchemeDTO getCodeScheme(final String codeRegistryCodeValue,
                                       final String codeSchemeCodeValue) {
        if (checkIfIndexExists(ELASTIC_INDEX_CODESCHEME)) {
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.sort("codeValue.raw", SortOrder.ASC);
            final BoolQueryBuilder builder = boolQuery().should(matchQuery("id", codeSchemeCodeValue.toLowerCase())).should(matchQuery("codeValue", codeSchemeCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER)).minimumShouldMatch(1);
            builder.must(matchQuery("codeRegistry.codeValue", codeRegistryCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER));
            searchBuilder.query(builder);
            return doCodeSchemeRequest(searchBuilder);
        }
        return null;
    }

    private CodeSchemeDTO doCodeSchemeRequest(final SearchSourceBuilder searchBuilder) {
        final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
        final SearchRequest searchRequest = createSearchRequest(ELASTIC_INDEX_CODESCHEME);
        searchRequest.source(searchBuilder);
        try {
            final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            if (response.getHits().getTotalHits() > 0) {
                LOG.debug(String.format("Found %d CodeSchemes", response.getHits().getTotalHits()));
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(), CodeSchemeDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getCodeScheme reading value from JSON string failed: " + hit.getSourceAsString(), e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            }
        } catch (final IOException e) {
            LOG.error("SearchRequest failed!", e);
            throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
        }
        return null;
    }

    public Set<CodeSchemeDTO> getCodeSchemesByCodeRegistryCodeValue(final String codeRegistryCodeValue,
                                                                    final List<String> organizations,
                                                                    final List<String> userOrganizationIds,
                                                                    final String language) {
        return getCodeSchemes(MAX_SIZE, 0, null, organizations, userOrganizationIds, codeRegistryCodeValue, null, null, null, language, null, false, false, null, null, null, null, null);
    }

    public Set<CodeSchemeDTO> getCodeSchemes(final String language) {
        return getCodeSchemes(MAX_SIZE, 0, null, null, null, null, null, null, null, language, null, false, false, null, null, null, null, null);
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
        final Set<String> codeSchemeUuidsWithDeepHitsCodes = new HashSet<>();
        final Set<String> codeSchemeUuidsWithDeepHitsExtensions = new HashSet<>();
        SearchResultWithMetaDataDTO searchResultWithMetaData = new SearchResultWithMetaDataDTO();

        if (searchCodes && searchTerm != null) {
            final Map<String, List<DeepSearchHitListDTO<?>>> deepSearchHits = getCodeSchemesMatchingCodes(searchTerm, searchResultWithMetaData);
            codeSchemeUuids.addAll(deepSearchHits.keySet());
            codeSchemeUuidsWithDeepHitsCodes.addAll(deepSearchHits.keySet());
        }

        if (searchExtensions && searchTerm != null) {
            Map<String, List<DeepSearchHitListDTO<?>>> deepSearchHits = getCodeSchemesMatchingExtensions(searchTerm, extensionPropertyType, searchResultWithMetaData);
            codeSchemeUuids.addAll(deepSearchHits.keySet());
            codeSchemeUuidsWithDeepHitsExtensions.addAll(deepSearchHits.keySet());
        }

        final Set<CodeSchemeDTO> codeSchemes = new LinkedHashSet<>();
        if (checkIfIndexExists(ELASTIC_INDEX_CODESCHEME)) {
            final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
            final SearchRequest searchRequest = createSearchRequest(ELASTIC_INDEX_CODESCHEME);
            final SearchSourceBuilder searchBuilder = createSearchSourceBuilderWithPagination(pageSize, from);
            final BoolQueryBuilder builder = boolQuery();
            if (searchTerm != null && !searchTerm.isEmpty()) {
                final BoolQueryBuilder boolQueryBuilder = boolQuery();
                boolQueryBuilder.should(luceneQueryFactory.buildPrefixSuffixQuery(searchTerm).field("prefLabel.*"));
                boolQueryBuilder.should(luceneQueryFactory.buildPrefixSuffixQuery(searchTerm).field("codeValue"));
                if (!codeSchemeUuids.isEmpty()) {
                    boolQueryBuilder.should(termsQuery("id", codeSchemeUuids));
                }
                boolQueryBuilder.minimumShouldMatch(1);
                builder.must(boolQueryBuilder);
            }
            if (codeSchemeCodeValue != null && !codeSchemeCodeValue.isEmpty()) {
                builder.must(luceneQueryFactory.buildPrefixSuffixQuery(codeSchemeCodeValue).field("codeValue"));
            }
            if (codeSchemePrefLabel != null && !codeSchemePrefLabel.isEmpty()) {
                builder.must(luceneQueryFactory.buildPrefixSuffixQuery(codeSchemePrefLabel).field("prefLabel.*"));
            }
            if (after != null) {
                final ISO8601DateFormat dateFormat = new ISO8601DateFormat();
                final String afterString = dateFormat.format(after);
                builder.must(rangeQuery("modified").gt(afterString));
            }
            if (organizationIds != null && !organizationIds.isEmpty()) {
                builder.must(nestedQuery("organizations", termsQuery("organizations.id.keyword", organizationIds), ScoreMode.None));
            }
            if (codeRegistryCodeValue != null && !codeRegistryCodeValue.isEmpty()) {
                builder.must(matchQuery("codeRegistry.codeValue", codeRegistryCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER));
            }
            if (codeRegistryPrefLabel != null && !codeRegistryPrefLabel.isEmpty()) {
                builder.must(luceneQueryFactory.buildPrefixSuffixQuery(codeRegistryPrefLabel).field("codeRegistry.prefLabel.*"));
            }
            if (infoDomains != null && !infoDomains.isEmpty()) {
                builder.must(nestedQuery("infoDomains", termsQuery("infoDomains.codeValue.keyword", infoDomains), ScoreMode.None));
            }
            if (extensionPropertyType != null) {
                builder.must(nestedQuery("extensions", matchQuery("extensions.propertyType.localName", extensionPropertyType), ScoreMode.None));
            }
            if (BOOSTSTATUS.equalsIgnoreCase(sortMode)) {
                searchBuilder.sort(SortBuilders.scoreSort());
                boostStatus(builder);
            }
            addLanguagePrefLabelSort(language, "codeValue.raw", "codeValue.raw", searchBuilder);
            if (statuses != null && !statuses.isEmpty()) {
                final BoolQueryBuilder boolQueryBuilder = boolQuery();
                if (statuses.contains(Status.INCOMPLETE.toString())) {
                    final BoolQueryBuilder unfinishedQueryBuilder = boolQuery();
                    unfinishedQueryBuilder.must(matchQuery("status.keyword", Status.INCOMPLETE.toString()));
                    unfinishedQueryBuilder.must(nestedQuery("organizations", termsQuery("organizations.id.keyword", userOrganizationIds), ScoreMode.None));
                    boolQueryBuilder.should(unfinishedQueryBuilder);
                    statuses.remove(Status.INCOMPLETE.toString());
                }
                boolQueryBuilder.should(termsQuery("status.keyword", statuses));
                builder.must(termsQuery("status.keyword", statuses));
                boolQueryBuilder.minimumShouldMatch(1);
                builder.must(boolQueryBuilder);
            } else {
                final BoolQueryBuilder boolQueryBuilder = boolQuery();
                boolQueryBuilder.should(termsQuery("status.keyword", getRegularStatuses()));
                if (userOrganizationIds != null && !userOrganizationIds.isEmpty()) {
                    final BoolQueryBuilder unfinishedQueryBuilder = boolQuery();
                    unfinishedQueryBuilder.must(matchQuery("status.keyword", Status.INCOMPLETE.toString()));
                    unfinishedQueryBuilder.must(nestedQuery("organizations", termsQuery("organizations.id.keyword", userOrganizationIds), ScoreMode.None));
                    boolQueryBuilder.should(unfinishedQueryBuilder);
                }
                boolQueryBuilder.minimumShouldMatch(1);
                builder.must(boolQueryBuilder);
            }
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                setResultCounts(meta, response);
                response.getHits().forEach(hit -> {
                    try {
                        codeSchemes.add(mapper.readValue(hit.getSourceAsString(), CodeSchemeDTO.class));
                    } catch (final IOException e) {
                        LOG.error("getCodeSchemes reading value from JSON string failed: " + hit.getSourceAsString(), e);
                        throw new JsonParsingException(ERR_MSG_USER_406);
                    }
                });
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
            }
        }
        for (final CodeSchemeDTO cs : codeSchemes) {
            final ArrayList<SearchHitDTO> searchHits = searchResultWithMetaData.getSearchHitDTOMap().get(cs.getId().toString().toLowerCase());
            if (language != null && searchHits != null) {
                searchHits.sort(Comparator.comparing(searchHitDTO -> searchHitDTO.getPrefLabel() != null && searchHitDTO.getPrefLabel().get(language) != null ? searchHitDTO.getPrefLabel().get(language) : searchHitDTO.getEntityCodeValue(), Comparator.nullsLast(Comparator.naturalOrder())));
            }
            cs.setSearchHits(searchHits);
            if (searchResultWithMetaData.getTotalhitsCodesPerCodeSchemeMap() != null && !searchResultWithMetaData.getTotalhitsCodesPerCodeSchemeMap().isEmpty() && codeSchemeUuidsWithDeepHitsCodes.contains(cs.getId().toString())) {
                cs.setTotalNrOfSearchHitsCodes(searchResultWithMetaData.getTotalhitsCodesPerCodeSchemeMap().get(cs.getId().toString()));
            }

            if (searchResultWithMetaData.getTotalhitsExtensionsPerCodeSchemeMap() != null && !searchResultWithMetaData.getTotalhitsExtensionsPerCodeSchemeMap().isEmpty() && codeSchemeUuidsWithDeepHitsExtensions.contains(cs.getId().toString())) {
                cs.setTotalNrOfSearchHitsExtensions(searchResultWithMetaData.getTotalhitsExtensionsPerCodeSchemeMap().get(cs.getId().toString()));
            }
        }
        return codeSchemes;
    }

    private Map<String, List<DeepSearchHitListDTO<?>>> getCodeSchemesMatchingCodes(final String searchTerm,
                                                                                   final SearchResultWithMetaDataDTO result) {
        final Map<String, List<DeepSearchHitListDTO<?>>> deepSearchHits;
        if (checkIfIndexExists(ELASTIC_INDEX_CODE) && searchTerm != null) {
            try {
                final SearchRequest query = deepCodeQueryFactory.createQuery(searchTerm);
                final SearchResponse response = client.search(query, RequestOptions.DEFAULT);
                deepSearchHits = deepCodeQueryFactory.parseResponse(response, result, searchTerm);
            } catch (final IOException e) {
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
            }
        } else {
            deepSearchHits = null;
        }
        return deepSearchHits;
    }

    private Map<String, List<DeepSearchHitListDTO<?>>> getCodeSchemesMatchingExtensions(final String searchTerm,
                                                                                        final String extensionPropertyType,
                                                                                        final SearchResultWithMetaDataDTO result) {
        final Map<String, List<DeepSearchHitListDTO<?>>> deepSearchHits;
        if (checkIfIndexExists(ELASTIC_INDEX_EXTENSION) && searchTerm != null) {
            try {
                final SearchRequest query = deepExtensionQueryFactory.createQuery(searchTerm, extensionPropertyType);
                final SearchResponse response = client.search(query, RequestOptions.DEFAULT);
                deepSearchHits = deepExtensionQueryFactory.parseResponse(response, result, searchTerm);
            } catch (final IOException e) {
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
            }
        } else {
            deepSearchHits = null;
        }
        return deepSearchHits;
    }

    private List<String> getRegularStatuses() {
        final List<String> allStatuses = new ArrayList<>();
        allStatuses.add(Status.DRAFT.toString());
        allStatuses.add(Status.SUGGESTED.toString());
        allStatuses.add(Status.VALID.toString());
        allStatuses.add(Status.INVALID.toString());
        allStatuses.add(Status.RETIRED.toString());
        allStatuses.add(Status.SUPERSEDED.toString());
        return allStatuses;
    }

    public CodeDTO getCode(final String codeId) {
        if (checkIfIndexExists(ELASTIC_INDEX_CODE)) {
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            final BoolQueryBuilder builder = boolQuery().must(matchQuery("id", codeId));
            searchBuilder.query(builder);
            return doCodeRequest(searchBuilder);
        } else {
            return null;
        }
    }

    public CodeDTO getCode(final String codeRegistryCodeValue,
                           final String codeSchemeCodeValue,
                           final String codeCodeValue) {
        if (checkIfIndexExists(ELASTIC_INDEX_CODE)) {
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            final BoolQueryBuilder builder = boolQuery().should(matchQuery("id", codeCodeValue.toLowerCase())).should(matchQuery("codeValue", codeCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER)).minimumShouldMatch(1);
            builder.must(boolQuery().should(matchQuery("codeScheme.id", codeSchemeCodeValue.toLowerCase())).should(matchQuery("codeScheme.codeValue", codeSchemeCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER)).minimumShouldMatch(1));
            builder.must(matchQuery("codeScheme.codeRegistry.codeValue", codeRegistryCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER));
            searchBuilder.query(builder);
            return doCodeRequest(searchBuilder);
        } else {
            return null;
        }
    }

    private CodeDTO doCodeRequest(final SearchSourceBuilder searchBuilder) {
        final SearchRequest searchRequest = createSearchRequest(ELASTIC_INDEX_CODE);
        searchRequest.source(searchBuilder);
        final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
        try {
            final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            LOG.debug(String.format("getCode found: %d hits.", response.getHits().getTotalHits()));
            if (response.getHits().getTotalHits() > 0) {
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(), CodeDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getCode reading value from JSON string failed: " + hit.getSourceAsString(), e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            }
        } catch (final IOException e) {
            LOG.error("SearchRequest failed!", e);
            throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
        }
        return null;
    }

    public Set<CodeDTO> getCodesByCodeRegistryCodeValueAndCodeSchemeCodeValue(final String codeRegistryCodeValue,
                                                                              final String codeSchemeCodeValue) {
        return getCodes(MAX_SIZE, 0, codeRegistryCodeValue, codeSchemeCodeValue, null, null, null, null, null, null, null, null);
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
            final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
            final SearchRequest searchRequest = createSearchRequest(ELASTIC_INDEX_CODE);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.size(pageSize != null ? pageSize : MAX_SIZE);
            searchBuilder.from(from != null ? from : 0);
            final BoolQueryBuilder builder = constructSearchQuery(codeCodeValue, prefLabel, after);
            builder.must(matchQuery("codeScheme.codeRegistry.codeValue", codeRegistryCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER));
            builder.must(boolQuery().should(matchQuery("codeScheme.codeValue", codeSchemeCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER)).should(matchQuery("codeScheme.id", codeSchemeCodeValue.toLowerCase())).minimumShouldMatch(1));
            if (hierarchyLevel != null) {
                builder.must(rangeQuery("hierarchyLevel").lte(hierarchyLevel));
            }
            if (broaderCodeId != null && !broaderCodeId.isEmpty()) {
                builder.must(matchQuery("broaderCode.id", broaderCodeId.toLowerCase()));
            }
            if (statuses != null && !statuses.isEmpty()) {
                builder.must(termsQuery("status.keyword", statuses));
            }
            addLanguagePrefLabelSort(language, "codeValue.raw", "order", searchBuilder);
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                setResultCounts(meta, response);
                response.getHits().forEach(hit -> {
                    try {
                        codes.add(mapper.readValue(hit.getSourceAsString(), CodeDTO.class));
                    } catch (final IOException e) {
                        LOG.error("getCodes reading value from JSON string failed: " + hit.getSourceAsString(), e);
                        throw new JsonParsingException(ERR_MSG_USER_406);
                    }
                });
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
            }
            return codes;
        }
        return codes;
    }

    public PropertyTypeDTO getPropertyType(final String propertyTypeIdentifier) {
        if (checkIfIndexExists(ELASTIC_INDEX_PROPERTYTYPE)) {
            final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
            final BoolQueryBuilder builder = new BoolQueryBuilder().should(matchQuery("id", propertyTypeIdentifier.toLowerCase())).should(matchQuery("localName", propertyTypeIdentifier.toLowerCase()).analyzer(TEXT_ANALYZER)).minimumShouldMatch(1);
            final SearchRequest searchRequest = createSearchRequestWithBoolQueryBuilder(ELASTIC_INDEX_PROPERTYTYPE, builder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                if (response.getHits().getTotalHits() > 0) {
                    final SearchHit hit = response.getHits().getAt(0);
                    try {
                        if (hit != null) {
                            return mapper.readValue(hit.getSourceAsString(), PropertyTypeDTO.class);
                        }
                    } catch (final IOException e) {
                        LOG.error("getPropertyType reading value from JSON string failed: " + hit.getSourceAsString(), e);
                        throw new JsonParsingException(ERR_MSG_USER_406);
                    }
                }
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
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
            final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
            final SearchRequest searchRequest = createSearchRequest(ELASTIC_INDEX_PROPERTYTYPE);
            final SearchSourceBuilder searchBuilder = createSearchSourceBuilderWithPagination(pageSize, from);
            final BoolQueryBuilder builder = constructSearchQuery(null, propertyTypePrefLabel, after);
            if (context != null) {
                builder.must(prefixQuery("context", context.toLowerCase()));
            }
            if (type != null) {
                builder.must(prefixQuery("type", type.toLowerCase()));
            }
            addLanguagePrefLabelSort(language, "localName.keyword", "localName.keyword", searchBuilder);
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                setResultCounts(meta, response);
                response.getHits().forEach(hit -> {
                    try {
                        final PropertyTypeDTO propertyType = mapper.readValue(hit.getSourceAsString(), PropertyTypeDTO.class);
                        propertyTypes.add(propertyType);
                    } catch (final IOException e) {
                        LOG.error("getPropertyTypes reading value from JSON string failed: " + hit.getSourceAsString(), e);
                        throw new JsonParsingException(ERR_MSG_USER_406);
                    }
                });
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
            }
        }
        return propertyTypes;
    }

    public ValueTypeDTO getValueType(final String valueTypeIdentifier) {
        if (checkIfIndexExists(ELASTIC_INDEX_VALUETYPE)) {
            final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
            final BoolQueryBuilder builder = new BoolQueryBuilder().should(matchQuery("id", valueTypeIdentifier.toLowerCase())).should(matchQuery("localName", valueTypeIdentifier.toLowerCase()).analyzer(TEXT_ANALYZER)).minimumShouldMatch(1);
            final SearchRequest searchRequest = createSearchRequestWithBoolQueryBuilder(ELASTIC_INDEX_PROPERTYTYPE, builder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                if (response.getHits().getTotalHits() > 0) {
                    final SearchHit hit = response.getHits().getAt(0);
                    try {
                        if (hit != null) {
                            return mapper.readValue(hit.getSourceAsString(), ValueTypeDTO.class);
                        }
                    } catch (final IOException e) {
                        LOG.error("getValueType reading value from JSON string failed: " + hit.getSourceAsString(), e);
                        throw new JsonParsingException(ERR_MSG_USER_406);
                    }
                }
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
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
            final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
            final SearchRequest searchRequest = createSearchRequest(ELASTIC_INDEX_VALUETYPE);
            final SearchSourceBuilder searchBuilder = createSearchSourceBuilderWithPagination(pageSize, from);
            final BoolQueryBuilder builder = constructSearchQuery(null, null, after);
            if (localName != null) {
                builder.must(prefixQuery("localName", localName.toLowerCase()));
            }
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                setResultCounts(meta, response);
                response.getHits().forEach(hit -> {
                    try {
                        final ValueTypeDTO valueType = mapper.readValue(hit.getSourceAsString(), ValueTypeDTO.class);
                        valueTypes.add(valueType);
                    } catch (final IOException e) {
                        LOG.error("getValueTypes reading value from JSON string failed: " + hit.getSourceAsString(), e);
                        throw new JsonParsingException(ERR_MSG_USER_406);
                    }
                });
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
            }
        }
        return valueTypes;
    }

    public ExternalReferenceDTO getExternalReference(final String externalReferenceId) {
        if (checkIfIndexExists(ELASTIC_INDEX_EXTERNALREFERENCE)) {
            final BoolQueryBuilder builder = boolQuery().must(matchQuery("id", externalReferenceId.toLowerCase()));
            final SearchRequest searchRequest = createSearchRequestWithBoolQueryBuilder(ELASTIC_INDEX_EXTERNALREFERENCE, builder);
            final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                if (response.getHits().getTotalHits() > 0) {
                    final SearchHit hit = response.getHits().getAt(0);
                    try {
                        if (hit != null) {
                            return mapper.readValue(hit.getSourceAsString(), ExternalReferenceDTO.class);
                        }
                    } catch (final IOException e) {
                        LOG.error("getExternalReference reading value from JSON string failed: " + hit.getSourceAsString(), e);
                        throw new JsonParsingException(ERR_MSG_USER_406);
                    }
                }
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
            }
        }
        return null;
    }

    public Set<ExternalReferenceDTO> getExternalReferences(final CodeSchemeDTO codeScheme) {
        return getExternalReferences(null, null, null, codeScheme, false, null, null);
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
            final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
            final SearchRequest searchRequest = createSearchRequest(ELASTIC_INDEX_EXTERNALREFERENCE);
            final SearchSourceBuilder searchBuilder = createSearchSourceBuilderWithPagination(pageSize, from);
            final BoolQueryBuilder builder = constructSearchQuery(null, externalReferencePrefLabel, after);
            if (codeScheme != null) {
                builder.should(boolQuery().should(boolQuery().must(matchQuery("parentCodeScheme.codeRegistry.codeValue", codeScheme.getCodeRegistry().getCodeValue().toLowerCase()).analyzer(TEXT_ANALYZER)).must(matchQuery("parentCodeScheme.id", codeScheme.getId().toString().toLowerCase()))).should(boolQuery().must(matchQuery("global", true))));
            } else if (!full) {
                builder.must(matchQuery("global", true));
            }
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                setResultCounts(meta, response);
                response.getHits().forEach(hit -> {
                    try {
                        final ExternalReferenceDTO externalReference = mapper.readValue(hit.getSourceAsString(), ExternalReferenceDTO.class);
                        externalReferences.add(externalReference);
                    } catch (final IOException e) {
                        LOG.error("getExternalReferences reading value from JSON string failed: " + hit.getSourceAsString(), e);
                        throw new JsonParsingException(ERR_MSG_USER_406);
                    }
                });
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
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
            final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
            final SearchRequest searchRequest = createSearchRequest(ELASTIC_INDEX_EXTENSION);
            final SearchSourceBuilder searchBuilder = createSearchSourceBuilderWithPagination(pageSize, from);
            searchBuilder.sort("codeValue.raw", SortOrder.ASC);
            final BoolQueryBuilder builder = constructSearchQuery(null, extensionPrefLabel, after);
            if (codeScheme != null) {
                builder.must(matchQuery("parentCodeScheme.id", codeScheme.getId().toString().toLowerCase()));
            }
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                setResultCounts(meta, response);
                response.getHits().forEach(hit -> {
                    try {
                        final ExtensionDTO extension = mapper.readValue(hit.getSourceAsString(), ExtensionDTO.class);
                        extensions.add(extension);
                    } catch (final IOException e) {
                        LOG.error("getExtensions reading value from JSON string failed: " + hit.getSourceAsString(), e);
                        throw new JsonParsingException(ERR_MSG_USER_406);
                    }
                });
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
            }
        }
        return extensions;
    }

    public ExtensionDTO getExtension(final String codeRegistryCodeValue,
                                     final String codeSchemeCodeValue,
                                     final String extensionCodeValue) {
        if (checkIfIndexExists(ELASTIC_INDEX_EXTENSION)) {
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.sort("codeValue.raw", SortOrder.ASC);
            final BoolQueryBuilder builder = boolQuery().should(matchQuery("id", extensionCodeValue.toLowerCase())).should(matchQuery("codeValue", extensionCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER)).minimumShouldMatch(1);
            builder.must(matchQuery("parentCodeScheme.codeRegistry.codeValue", codeRegistryCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER));
            builder.must(matchQuery("parentCodeScheme.codeValue", codeSchemeCodeValue.toLowerCase()).analyzer(TEXT_ANALYZER));
            searchBuilder.query(builder);
            return doExtensionRequest(searchBuilder);
        }
        return null;
    }

    public ExtensionDTO getExtension(final String extensionId) {
        if (checkIfIndexExists(ELASTIC_INDEX_EXTENSION)) {
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            final BoolQueryBuilder builder = boolQuery().must(matchQuery("id", extensionId.toLowerCase()));
            searchBuilder.query(builder);
            return doExtensionRequest(searchBuilder);
        }
        return null;
    }

    private ExtensionDTO doExtensionRequest(final SearchSourceBuilder searchBuilder) {
        final SearchRequest searchRequest = createSearchRequest(ELASTIC_INDEX_EXTENSION);
        searchRequest.source(searchBuilder);
        final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
        try {
            final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            if (response.getHits().getTotalHits() > 0) {
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(), ExtensionDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getExtension reading value from JSON string failed: " + hit.getSourceAsString(), e);
                    throw new JsonParsingException(ERR_MSG_USER_406);
                }
            }
        } catch (final IOException e) {
            LOG.error("SearchRequest failed!", e);
            throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
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
            final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
            final SearchRequest searchRequest = createSearchRequest(ELASTIC_INDEX_MEMBER);
            final SearchSourceBuilder searchBuilder = createSearchSourceBuilderWithPagination(pageSize, from);
            searchBuilder.sort("order", SortOrder.ASC);
            final BoolQueryBuilder builder = constructSearchQuery(null, null, after);
            if (code != null) {
                builder.must(matchQuery("code.id", code.getId().toString().toLowerCase()));
            }
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                setResultCounts(meta, response);
                response.getHits().forEach(hit -> {
                    try {
                        final MemberDTO member = mapper.readValue(hit.getSourceAsString(), MemberDTO.class);
                        members.add(member);
                    } catch (final IOException e) {
                        LOG.error("getMembers reading value from JSON string failed: " + hit.getSourceAsString(), e);
                        throw new JsonParsingException(ERR_MSG_USER_406);
                    }
                });
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
            }
        }
        return members;
    }

    public Set<MemberDTO> getMembers(final Integer pageSize,
                                     final Integer from,
                                     final Date after,
                                     final Meta meta) {
        validatePageSize(pageSize);
        final Set<MemberDTO> members;
        if (checkIfIndexExists(ELASTIC_INDEX_MEMBER)) {
            final SearchSourceBuilder searchBuilder = createSearchSourceBuilderWithPagination(pageSize, from);
            searchBuilder.sort("order", SortOrder.ASC);
            final BoolQueryBuilder builder = constructSearchQuery(null, null, after);
            searchBuilder.query(builder);
            members = doMemberRequest(searchBuilder, meta);
        } else {
            members = new LinkedHashSet<>();
        }
        return members;
    }

    public Set<MemberDTO> getMembers(final Integer pageSize,
                                     final Integer from,
                                     final ExtensionDTO extension,
                                     final Date after,
                                     final Meta meta) {
        final Set<MemberDTO> members;
        validatePageSize(pageSize);
        if (checkIfIndexExists(ELASTIC_INDEX_MEMBER)) {
            final SearchSourceBuilder searchBuilder = createSearchSourceBuilderWithPagination(pageSize, from);
            searchBuilder.sort("order", SortOrder.ASC);
            final BoolQueryBuilder builder = constructSearchQuery(null, null, after);
            searchBuilder.query(builder);
            if (extension != null) {
                builder.must(matchQuery("extension.id", extension.getId().toString().toLowerCase()));
            }
            members = doMemberRequest(searchBuilder, meta);
        } else {
            members = new LinkedHashSet<>();
        }
        return members;
    }

    private Set<MemberDTO> doMemberRequest(final SearchSourceBuilder searchBuilder,
                                           final Meta meta) {
        final Set<MemberDTO> members = new LinkedHashSet<>();
        final SearchRequest searchRequest = createSearchRequest(ELASTIC_INDEX_MEMBER);
        searchRequest.source(searchBuilder);
        final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
        try {
            final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            setResultCounts(meta, response);
            response.getHits().forEach(hit -> {
                try {
                    final MemberDTO member = mapper.readValue(hit.getSourceAsString(), MemberDTO.class);
                    members.add(member);
                } catch (final IOException e) {
                    LOG.error("getMembers reading value from JSON string failed: " + hit.getSourceAsString(), e);
                }
            });
        } catch (final IOException e) {
            LOG.error("SearchRequest failed!", e);
            throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
        }
        return members;
    }

    @SuppressWarnings("unused")
    @SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
    public MemberDTO getMember(final String memberId,
                               final String extensionCodeValue) {
        boolean memberIdIsUUID = true;
        try {
            final UUID theUuid = UUID.fromString(memberId);
        } catch (final Exception e) {
            memberIdIsUUID = false;
        }
        if (checkIfIndexExists(ELASTIC_INDEX_MEMBER)) {
            final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
            final SearchRequest searchRequest = createSearchRequest(ELASTIC_INDEX_MEMBER);
            final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            if (memberIdIsUUID) {
                final BoolQueryBuilder builder = boolQuery().must(matchQuery("id", memberId.toLowerCase()));
                searchBuilder.query(builder);
                searchRequest.source(searchBuilder);
            } else {
                final BoolQueryBuilder builder = boolQuery().must(matchQuery("sequenceId", memberId)).must(matchQuery("extension.codeValue", extensionCodeValue));
                searchBuilder.query(builder);
                searchRequest.source(searchBuilder);
            }
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                if (response.getHits().getTotalHits() > 0) {
                    final SearchHit hit = response.getHits().getAt(0);
                    try {
                        if (hit != null) {
                            return mapper.readValue(hit.getSourceAsString(), MemberDTO.class);
                        }
                    } catch (final IOException e) {
                        LOG.error("getMember reading value from JSON string failed: " + hit.getSourceAsString(), e);
                    }
                }
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
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
            final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
            final SearchRequest searchRequest = createSearchRequest(ELASTIC_INDEX_CODESCHEME);
            final SearchSourceBuilder searchBuilder = createSearchSourceBuilderWithPagination(pageSize, from);
            final BoolQueryBuilder builder = boolQuery();
            if (after != null) {
                final ISO8601DateFormat dateFormat = new ISO8601DateFormat();
                final String afterString = dateFormat.format(after);
                builder.must(rangeQuery("modified").gt(afterString));
            }
            addLanguagePrefLabelSort(language, "codeValue.raw", "codeValue.raw", searchBuilder);
            if (statuses != null && !statuses.isEmpty()) {
                final BoolQueryBuilder boolQueryBuilder = boolQuery();
                if (statuses.contains(Status.INCOMPLETE.toString())) {
                    final BoolQueryBuilder unfinishedQueryBuilder = boolQuery();
                    unfinishedQueryBuilder.must(matchQuery("status.keyword", Status.INCOMPLETE.toString()));
                    boolQueryBuilder.should(unfinishedQueryBuilder);
                    statuses.remove(Status.INCOMPLETE.toString());
                }
                boolQueryBuilder.should(termsQuery("status.keyword", statuses));
                builder.must(termsQuery("status.keyword", statuses));
                boolQueryBuilder.minimumShouldMatch(1);
                builder.must(boolQueryBuilder);
            } else {
                final BoolQueryBuilder boolQueryBuilder = boolQuery();
                boolQueryBuilder.should(termsQuery("status.keyword", getRegularStatuses()));
                boolQueryBuilder.minimumShouldMatch(1);
                builder.must(boolQueryBuilder);
            }
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                setResultCounts(meta, response);
                response.getHits().forEach(hit -> {
                    try {
                        final CodeSchemeDTO codeSchemeDto = mapper.readValue(hit.getSourceAsString(), CodeSchemeDTO.class);
                        containers.add(new ResourceDTO(codeSchemeDto));
                    } catch (final IOException e) {
                        LOG.error("getContainers reading value from JSON string failed: " + hit.getSourceAsString(), e);
                        throw new JsonParsingException(ERR_MSG_USER_406);
                    }
                });
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
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
            final ObjectMapper mapper = createObjectMapperWithRegisteredModules();
            final SearchRequest searchRequest = createSearchRequest(ELASTIC_INDEX_CODE);
            final SearchSourceBuilder searchBuilder = createSearchSourceBuilderWithPagination(pageSize, from);
            final BoolQueryBuilder builder = constructSearchQuery(null, null, after);
            builder.must(matchQuery("codeScheme.uri", codeSchemeUri.toLowerCase()).analyzer(TEXT_ANALYZER));
            if (statuses != null && !statuses.isEmpty()) {
                builder.must(termsQuery("status.keyword", statuses));
            }
            addLanguagePrefLabelSort(language, "codeValue.raw", "order", searchBuilder);
            searchBuilder.query(builder);
            searchRequest.source(searchBuilder);
            try {
                final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                setResultCounts(meta, response);
                response.getHits().forEach(hit -> {
                    try {
                        final CodeDTO codeSchemeDto = mapper.readValue(hit.getSourceAsString(), CodeDTO.class);
                        resources.add(new ResourceDTO(codeSchemeDto));
                    } catch (final IOException e) {
                        LOG.error("getResources reading value from JSON string failed: " + hit.getSourceAsString(), e);
                        throw new JsonParsingException(ERR_MSG_USER_406);
                    }
                });
                return resources;
            } catch (final IOException e) {
                LOG.error("SearchRequest failed!", e);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
            }

        }
        return resources;
    }

    private BoolQueryBuilder constructSearchQuery(final String codeValue,
                                                  final String prefLabel,
                                                  final Date after) {
        final BoolQueryBuilder builder = boolQuery();
        if (codeValue != null) {
            builder.must(prefixQuery("codeValue", codeValue.toLowerCase()));
        }
        if (prefLabel != null) {
            builder.must(luceneQueryFactory.buildPrefixSuffixQuery(prefLabel).field("prefLabel.*"));
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
        LOG.debug(String.format("Search found: %d total hits.", totalResults));
    }

    private void boostStatus(final BoolQueryBuilder builder) {
        builder.should(constantScoreQuery(termQuery("status.keyword", Status.VALID.toString())).boost(1000f));
        builder.should(constantScoreQuery(termQuery("status.keyword", Status.DRAFT.toString())).boost(800f));
        builder.should(constantScoreQuery(termQuery("status.keyword", Status.SUGGESTED.toString())).boost(700f));
        builder.should(constantScoreQuery(termQuery("status.keyword", Status.SUPERSEDED.toString())).boost(600f));
        builder.should(constantScoreQuery(termQuery("status.keyword", Status.RETIRED.toString())).boost(500f));
        builder.should(constantScoreQuery(termQuery("status.keyword", Status.INVALID.toString())).boost(400f));
        builder.should(constantScoreQuery(termQuery("status.keyword", Status.INCOMPLETE.toString())).boost(300f));
    }

    private void addLanguagePrefLabelSort(final String language,
                                          final String backupSortField,
                                          final String sortFieldWithoutLanguage,
                                          final SearchSourceBuilder searchBuilder) {
        if (language != null && !language.isEmpty()) {
            searchBuilder.sort(SortBuilders.fieldSort("prefLabel." + language + ".keyword").order(SortOrder.ASC).unmappedType("keyword"));
            sortLanguages.forEach(sortLanguage -> {
                if (!language.equalsIgnoreCase(sortLanguage)) {
                    searchBuilder.sort(SortBuilders.fieldSort("prefLabel." + sortLanguage + ".keyword").order(SortOrder.ASC).unmappedType("keyword"));
                }
            });
            searchBuilder.sort(backupSortField, SortOrder.ASC);
        } else {
            searchBuilder.sort(sortFieldWithoutLanguage, SortOrder.ASC);
        }
    }

    private void validatePageSize(final Integer pageSize) {
        if (pageSize != null && pageSize > MAX_SIZE) {
            throw new YtiCodeListException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), String.format("Paging pageSize parameter value %d exceeds max value %d.", pageSize, MAX_SIZE)));
        }
    }

    private ObjectMapper createObjectMapperWithRegisteredModules() {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return objectMapper;
    }

    private boolean checkIfIndexExists(final String indexName) {
        final GetIndexRequest request = new GetIndexRequest();
        request.indices(indexName);
        try {
            return client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (final IOException e) {
            LOG.error("Index checking request failed for index: " + indexName, e);
            throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
        }
    }

    private SearchSourceBuilder createSearchSourceBuilderWithPagination(final Integer pageSize,
                                                                        final Integer from) {
        final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        searchBuilder.size(pageSize != null ? pageSize : MAX_SIZE);
        searchBuilder.from(from != null ? from : 0);
        return searchBuilder;
    }

    private SearchRequest createSearchRequestWithBoolQueryBuilder(final String indexName,
                                                                  final BoolQueryBuilder builder) {
        final SearchRequest searchRequest = createSearchRequest(indexName);
        final SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        searchBuilder.query(builder);
        searchRequest.source(searchBuilder);
        return searchRequest;
    }

    private SearchRequest createSearchRequest(final String indexName,
                                              final String typeName) {
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indexName);
        searchRequest.types(typeName);
        return searchRequest;
    }

    private SearchRequest createSearchRequest(final String indexName) {
        switch (indexName) {
            case ELASTIC_INDEX_CODEREGISTRY: {
                return createSearchRequest(ELASTIC_INDEX_CODEREGISTRY, ELASTIC_TYPE_CODEREGISTRY);
            }
            case ELASTIC_INDEX_CODESCHEME: {
                return createSearchRequest(ELASTIC_INDEX_CODESCHEME, ELASTIC_TYPE_CODESCHEME);
            }
            case ELASTIC_INDEX_CODE: {
                return createSearchRequest(ELASTIC_INDEX_CODE, ELASTIC_TYPE_CODE);
            }
            case ELASTIC_INDEX_EXTENSION: {
                return createSearchRequest(ELASTIC_INDEX_EXTENSION, ELASTIC_TYPE_EXTENSION);
            }
            case ELASTIC_INDEX_MEMBER: {
                return createSearchRequest(ELASTIC_INDEX_MEMBER, ELASTIC_TYPE_MEMBER);
            }
            case ELASTIC_INDEX_EXTERNALREFERENCE: {
                return createSearchRequest(ELASTIC_INDEX_EXTERNALREFERENCE, ELASTIC_TYPE_EXTERNALREFERENCE);
            }
            case ELASTIC_INDEX_PROPERTYTYPE: {
                return createSearchRequest(ELASTIC_INDEX_PROPERTYTYPE, ELASTIC_TYPE_PROPERTYTYPE);
            }
            case ELASTIC_INDEX_VALUETYPE: {
                return createSearchRequest(ELASTIC_INDEX_VALUETYPE, ELASTIC_TYPE_VALUETYPE);
            }
            default: {
                LOG.error("Trying to create search request with non-supported index: " + indexName);
                throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), ELASTIC_QUERY_ERROR));
            }
        }
    }
}
