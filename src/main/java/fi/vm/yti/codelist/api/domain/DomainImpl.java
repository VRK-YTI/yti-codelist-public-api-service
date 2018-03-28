package fi.vm.yti.codelist.api.domain;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;

import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.CodeRegistryDTO;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ExternalReferenceDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import fi.vm.yti.codelist.common.dto.PropertyTypeDTO;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;
import static java.lang.Math.toIntExact;
import static org.elasticsearch.index.query.QueryBuilders.*;

@Singleton
@Service
public class DomainImpl implements Domain {

    private static final Logger LOG = LoggerFactory.getLogger(DomainImpl.class);
    private static final int MAX_SIZE = 10000;
    private static final String ANALYZER_KEYWORD = "analyzer_keyword";
    private Client client;

    @Inject
    private DomainImpl(final Client client) {
        this.client = client;
    }

    public CodeRegistryDTO getCodeRegistry(final String codeRegistryCodeValue) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_CODEREGISTRY).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODEREGISTRY)
                .setTypes(ELASTIC_TYPE_CODEREGISTRY)
                .addSort("codeValue.raw", SortOrder.ASC);
            final BoolQueryBuilder builder = boolQuery()
                .should(matchQuery("id", codeRegistryCodeValue.toLowerCase()))
                .should(matchQuery("codeValue", codeRegistryCodeValue.toLowerCase()).analyzer(ANALYZER_KEYWORD))
                .minimumShouldMatch(1);
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            if (response.getHits().getTotalHits() > 0) {
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(), CodeRegistryDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getCodeRegistry reading value from JSON string failed: " + hit.getSourceAsString(), e);
                }
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
        final Set<CodeRegistryDTO> codeRegistries = new LinkedHashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_CODEREGISTRY).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODEREGISTRY)
                .setTypes(ELASTIC_TYPE_CODEREGISTRY)
                .addSort("codeValue.raw", SortOrder.ASC)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0);

            final BoolQueryBuilder builder = constructSearchQuery(codeRegistryCodeValue, codeRegistryPrefLabel, after);
            if (organizations != null && !organizations.isEmpty()) {
                builder.must(termsQuery("organizations.id.keyword", organizations));
            }
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta, response);
            response.getHits().forEach(hit -> {
                try {
                    codeRegistries.add(mapper.readValue(hit.getSourceAsString(), CodeRegistryDTO.class));
                } catch (final IOException e) {
                    LOG.error("getCodeRegistries reading value from JSON string failed: " + hit.getSourceAsString(), e);
                }
            });
        }
        return codeRegistries;
    }

    public CodeSchemeDTO getCodeScheme(final String codeSchemeId) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_CODESCHEME).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODESCHEME)
                .setTypes(ELASTIC_TYPE_CODESCHEME)
                .addSort("codeValue.raw", SortOrder.ASC);
            final BoolQueryBuilder builder = boolQuery()
                .must(matchQuery("id", codeSchemeId.toLowerCase()));
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            if (response.getHits().getTotalHits() > 0) {
                LOG.info("Found " + response.getHits().getTotalHits() + " CodeSchemes");
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(), CodeSchemeDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getCodeScheme reading value from JSON string failed: " + hit.getSourceAsString(), e);
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
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODESCHEME)
                .setTypes(ELASTIC_TYPE_CODESCHEME)
                .addSort("codeValue.raw", SortOrder.ASC);
            final BoolQueryBuilder builder = boolQuery()
                .should(matchQuery("id", codeSchemeCodeValue.toLowerCase()))
                .should(matchQuery("codeValue", codeSchemeCodeValue.toLowerCase()).analyzer(ANALYZER_KEYWORD))
                .minimumShouldMatch(1);
            builder.must(matchQuery("codeRegistry.codeValue", codeRegistryCodeValue.toLowerCase()).analyzer(ANALYZER_KEYWORD));
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            if (response.getHits().getTotalHits() > 0) {
                LOG.info("Found " + response.getHits().getTotalHits() + " CodeSchemes");
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(), CodeSchemeDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getCodeScheme reading value from JSON string failed: " + hit.getSourceAsString(), e);
                }
            }
        }
        return null;
    }

    public Set<CodeSchemeDTO> getCodeSchemesByCodeRegistryCodeValue(final String codeRegistryCodeValue) {
        return getCodeSchemes(MAX_SIZE, 0, null, codeRegistryCodeValue, null, null, null, null, null, null, null, null);
    }

    public Set<CodeSchemeDTO> getCodeSchemes() {
        return getCodeSchemes(MAX_SIZE, 0, null, null, null, null, null, null, null, null, null, null);
    }

    public Set<CodeSchemeDTO> getCodeSchemes(final Integer pageSize,
                                             final Integer from,
                                             final String organizationId,
                                             final String codeRegistryCodeValue,
                                             final String codeRegistryPrefLabel,
                                             final String codeSchemeCodeValue,
                                             final String codeSchemePrefLabel,
                                             final String searchTerm,
                                             final List<String> statuses,
                                             final List<String> dataClassifications,
                                             final Date after,
                                             final Meta meta) {
        final Set<CodeSchemeDTO> codeSchemes = new LinkedHashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_CODESCHEME).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODESCHEME)
                .setTypes(ELASTIC_TYPE_CODESCHEME)
                .addSort("codeValue.raw", SortOrder.ASC)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0);
            final BoolQueryBuilder builder = constructCombinedSearchQuery(searchTerm, codeSchemeCodeValue, codeSchemePrefLabel, after);
            if (organizationId != null) {
                builder.must(nestedQuery("codeRegistry.organizations", matchQuery("codeRegistry.organizations.id", organizationId.toLowerCase()), ScoreMode.None));
            }
            if (codeRegistryCodeValue != null) {
                builder.must(matchQuery("codeRegistry.codeValue", codeRegistryCodeValue.toLowerCase()).analyzer(ANALYZER_KEYWORD));
            }
            if (codeRegistryPrefLabel != null) {
                builder.must(nestedQuery("codeRegistry.prefLabel", multiMatchQuery(codeRegistryPrefLabel.toLowerCase() + "*", "prefLabel.*").type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX), ScoreMode.None));
            }
            if (dataClassifications != null && !dataClassifications.isEmpty()) {
                builder.must(nestedQuery("dataClassifications", matchQuery("dataClassifications.codeValue", dataClassifications), ScoreMode.None));
            }
            if (statuses != null && !statuses.isEmpty()) {
                builder.must(termsQuery("status.keyword", statuses));
            }
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta, response);
            response.getHits().forEach(hit -> {
                try {
                    codeSchemes.add(mapper.readValue(hit.getSourceAsString(), CodeSchemeDTO.class));
                } catch (final IOException e) {
                    LOG.error("getCodeSchemes reading value from JSON string failed: " + hit.getSourceAsString(), e);
                }
            });
        }
        return codeSchemes;
    }

    public CodeDTO getCode(final String codeRegistryCodeValue,
                           final String codeSchemeCodeValue,
                           final String codeCodeValue) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_CODE).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODE)
                .setTypes(ELASTIC_TYPE_CODE);
            final BoolQueryBuilder builder = boolQuery()
                .should(matchQuery("id", codeCodeValue.toLowerCase()))
                .should(matchQuery("codeValue", codeCodeValue.toLowerCase()).analyzer(ANALYZER_KEYWORD))
                .minimumShouldMatch(1);
            builder.must(boolQuery()
                .should(matchQuery("codeScheme.id", codeSchemeCodeValue.toLowerCase()))
                .should(matchQuery("codeScheme.codeValue", codeSchemeCodeValue.toLowerCase()).analyzer(ANALYZER_KEYWORD))
                .minimumShouldMatch(1));
            builder.must(matchQuery("codeScheme.codeRegistry.codeValue", codeRegistryCodeValue.toLowerCase()).analyzer(ANALYZER_KEYWORD));
            searchRequest.setQuery(builder);

            final SearchResponse response = searchRequest.execute().actionGet();
            LOG.info("getCode found: " + response.getHits().getTotalHits() + " hits.");
            if (response.getHits().getTotalHits() > 0) {
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(), CodeDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getCode reading value from JSON string failed: " + hit.getSourceAsString(), e);
                }
            }
            return null;
        } else {
            return null;
        }
    }

    public Set<CodeDTO> getCodesByCodeRegistryCodeValueAndCodeSchemeCodeValue(final String codeRegistryCodeValue,
                                                                              final String codeSchemeCodeValue) {
        return getCodes(MAX_SIZE, 0, codeRegistryCodeValue, codeSchemeCodeValue, null, null, null, null, null, null, null);
    }

    public Set<CodeDTO> getCodes(final Integer pageSize,
                                 final Integer from,
                                 final String codeRegistryCodeValue,
                                 final String codeSchemeCodeValue,
                                 final String codeCodeValue,
                                 final String prefLabel,
                                 final Integer hierarchyLevel,
                                 final String broaderCodeId,
                                 final List<String> statuses,
                                 final Date after,
                                 final Meta meta) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_CODE).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            final Set<CodeDTO> codes = new LinkedHashSet<>();
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_CODE)
                .setTypes(ELASTIC_TYPE_CODE)
                .addSort("codeValue.raw", SortOrder.ASC)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0);

            final BoolQueryBuilder builder = constructSearchQuery(codeCodeValue, prefLabel, after);
            builder.must(matchQuery("codeScheme.codeRegistry.codeValue", codeRegistryCodeValue.toLowerCase()).analyzer(ANALYZER_KEYWORD));
            builder.must(boolQuery()
                .should(matchQuery("codeScheme.codeValue", codeSchemeCodeValue.toLowerCase()).analyzer(ANALYZER_KEYWORD))
                .should(matchQuery("codeScheme.id", codeSchemeCodeValue.toLowerCase()))
                .minimumShouldMatch(1));
            searchRequest.setQuery(builder);
            if (hierarchyLevel != null) {
                builder.must(rangeQuery("hierarchyLevel").lte(hierarchyLevel));
            }
            if (broaderCodeId != null && !broaderCodeId.isEmpty()) {
                builder.must(matchQuery("broaderCodeId", broaderCodeId.toLowerCase()));
            }
            if (statuses != null && !statuses.isEmpty()) {
                builder.must(termsQuery("status.keyword", statuses));
            }
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta, response);
            response.getHits().forEach(hit -> {
                try {
                    codes.add(mapper.readValue(hit.getSourceAsString(), CodeDTO.class));
                } catch (final IOException e) {
                    LOG.error("getCodes reading value from JSON string failed: " + hit.getSourceAsString(), e);
                }
            });
            return codes;
        }
        return null;
    }

    public PropertyTypeDTO getPropertyType(final String propertyTypeId) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_PROPERTYTYPE).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_PROPERTYTYPE)
                .setTypes(ELASTIC_TYPE_PROPERTYTYPE);
            final BoolQueryBuilder builder = boolQuery()
                .must(matchQuery("id", propertyTypeId.toLowerCase()));
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            if (response.getHits().getTotalHits() > 0) {
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(), PropertyTypeDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getPropertyType reading value from JSON string failed: " + hit.getSourceAsString(), e);
                }
            }
        }
        return null;
    }

    public Set<PropertyTypeDTO> getPropertyTypes() {
        return getPropertyTypes(MAX_SIZE, 0, null, null, null, null);
    }

    public Set<PropertyTypeDTO> getPropertyTypes(final Integer pageSize,
                                                 final Integer from,
                                                 final String propertyTypePrefLabel,
                                                 final String context,
                                                 final Date after,
                                                 final Meta meta) {
        final Set<PropertyTypeDTO> propertyTypes = new LinkedHashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_PROPERTYTYPE).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_PROPERTYTYPE)
                .setTypes(ELASTIC_TYPE_PROPERTYTYPE)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0);
            final BoolQueryBuilder builder = constructSearchQuery(null, propertyTypePrefLabel, after);
            if (context != null) {
                builder.must(prefixQuery("context", context.toLowerCase()));
            }
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta, response);
            response.getHits().forEach(hit -> {
                try {
                    final PropertyTypeDTO propertyType = mapper.readValue(hit.getSourceAsString(), PropertyTypeDTO.class);
                    propertyTypes.add(propertyType);
                } catch (final IOException e) {
                    LOG.error("getPropertyTypes reading value from JSON string failed: " + hit.getSourceAsString(), e);
                }
            });
        }
        return propertyTypes;
    }

    public ExternalReferenceDTO getExternalReference(final String externalReferenceId) {
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_EXTERNALREFERENCE).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_EXTERNALREFERENCE)
                .setTypes(ELASTIC_TYPE_EXTERNALREFERENCE);
            final BoolQueryBuilder builder = boolQuery()
                .must(matchQuery("id", externalReferenceId.toLowerCase()));
            searchRequest.setQuery(builder);
            final SearchResponse response = searchRequest.execute().actionGet();
            if (response.getHits().getTotalHits() > 0) {
                final SearchHit hit = response.getHits().getAt(0);
                try {
                    if (hit != null) {
                        return mapper.readValue(hit.getSourceAsString(), ExternalReferenceDTO.class);
                    }
                } catch (final IOException e) {
                    LOG.error("getExternalReference reading value from JSON string failed: " + hit.getSourceAsString(), e);
                }
            }
        }
        return null;
    }

    public Set<ExternalReferenceDTO> getExternalReferences() {
        return getExternalReferences(MAX_SIZE, 0, null, null, null, null);
    }

    public Set<ExternalReferenceDTO> getExternalReferences(final Integer pageSize,
                                                           final Integer from,
                                                           final String externalReferencePrefLabel,
                                                           final CodeSchemeDTO codeScheme,
                                                           final Date after,
                                                           final Meta meta) {
        final Set<ExternalReferenceDTO> externalReferences = new LinkedHashSet<>();
        final boolean exists = client.admin().indices().prepareExists(ELASTIC_INDEX_EXTERNALREFERENCE).execute().actionGet().isExists();
        if (exists) {
            final ObjectMapper mapper = new ObjectMapper();
            final SearchRequestBuilder searchRequest = client
                .prepareSearch(ELASTIC_INDEX_EXTERNALREFERENCE)
                .setTypes(ELASTIC_TYPE_EXTERNALREFERENCE)
                .setSize(pageSize != null ? pageSize : MAX_SIZE)
                .setFrom(from != null ? from : 0);
            final BoolQueryBuilder builder = constructSearchQuery(null, externalReferencePrefLabel, after);
            searchRequest.setQuery(builder);
            if (codeScheme != null) {
                builder.should(boolQuery()
                    .should(boolQuery()
                        .must(matchQuery("parentCodeScheme.codeRegistry.codeValue", codeScheme.getCodeRegistry().getCodeValue().toLowerCase()).analyzer(ANALYZER_KEYWORD))
                        .must(matchQuery("parentCodeScheme.id", codeScheme.getId().toString().toLowerCase())))
                    .should(boolQuery()
                        .must(matchQuery("global", true))));

            }
            final SearchResponse response = searchRequest.execute().actionGet();
            setResultCounts(meta, response);
            response.getHits().forEach(hit -> {
                try {
                    final ExternalReferenceDTO externalReference = mapper.readValue(hit.getSourceAsString(), ExternalReferenceDTO.class);
                    externalReferences.add(externalReference);
                } catch (final IOException e) {
                    LOG.error("getExternalReferences reading value from JSON string failed: " + hit.getSourceAsString(), e);
                }
            });
        }
        return externalReferences;
    }

    private BoolQueryBuilder constructCombinedSearchQuery(final String searchQuery,
                                                          final String codeValue,
                                                          final String prefLabel,
                                                          final Date after) {
        final BoolQueryBuilder builder = boolQuery();
        final BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        if (searchQuery != null) {
            boolQueryBuilder.should(prefixQuery("codeValue", searchQuery.toLowerCase()));
            boolQueryBuilder.should(nestedQuery("prefLabel", multiMatchQuery(searchQuery.toLowerCase() + "*", "prefLabel.*").type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX), ScoreMode.None));
            boolQueryBuilder.minimumShouldMatch(1);
            builder.must(boolQueryBuilder);
        }
        if (codeValue != null) {
            builder.must(prefixQuery("codeValue", codeValue.toLowerCase()));
        }
        if (prefLabel != null) {
            builder.must(nestedQuery("prefLabel", multiMatchQuery(prefLabel.toLowerCase() + "*", "prefLabel.*").type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX), ScoreMode.None));
        }
        if (after != null) {
            final ISO8601DateFormat dateFormat = new ISO8601DateFormat();
            final String afterString = dateFormat.format(after);
            builder.must(rangeQuery("modified").gt(afterString));
        }
        return builder;
    }

    private BoolQueryBuilder constructSearchQuery(final String codeValue,
                                                  final String prefLabel,
                                                  final Date after) {
        final BoolQueryBuilder builder = boolQuery();
        if (codeValue != null) {
            builder.must(prefixQuery("codeValue", codeValue.toLowerCase()));
        }
        if (prefLabel != null) {
            builder.must(nestedQuery("prefLabel", multiMatchQuery(prefLabel.toLowerCase() + "*", "prefLabel.*").type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX), ScoreMode.None));
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
        LOG.info("Search found: " + totalResults + " total hits.");
    }
}
