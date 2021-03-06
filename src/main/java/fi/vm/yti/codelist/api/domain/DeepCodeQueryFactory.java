package fi.vm.yti.codelist.api.domain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.DeepSearchCodeHitListDTO;
import fi.vm.yti.codelist.common.dto.DeepSearchHitListDTO;
import fi.vm.yti.codelist.common.dto.SearchHitDTO;
import fi.vm.yti.codelist.common.dto.SearchResultWithMetaDataDTO;
import static fi.vm.yti.codelist.common.constants.ApiConstants.ELASTIC_INDEX_CODE;
import static fi.vm.yti.codelist.common.constants.ApiConstants.SEARCH_HIT_TYPE_CODE;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

class DeepCodeQueryFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DeepCodeQueryFactory.class);

    private static final FetchSourceContext sourceIncludes = new FetchSourceContext(true, new String[]{ "id", "uri", "status", "codeValue", "prefLabel", "codeScheme.id" }, new String[]{});
    private static final Script topHitScript = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "_score", Collections.emptyMap());
    private final Domain domain;
    private final ObjectMapper objectMapper;
    private final LuceneQueryFactory luceneQueryFactory;

    DeepCodeQueryFactory(final ObjectMapper objectMapper,
                         final Domain domain,
                         final LuceneQueryFactory luceneQueryFactory) {
        this.objectMapper = objectMapper;
        this.domain = domain;
        this.luceneQueryFactory = luceneQueryFactory;
    }

    SearchRequest createQuery(final String query) {
        final BoolQueryBuilder boolQueryBuilder = boolQuery();
        if (query != null && !query.isEmpty()) {
            boolQueryBuilder.should(luceneQueryFactory.buildPrefixSuffixQuery(query).field("prefLabel.*"));
            boolQueryBuilder.should(luceneQueryFactory.buildPrefixSuffixQuery(query).field("codeValue"));
        }
        boolQueryBuilder.minimumShouldMatch(1);

        return new SearchRequest(ELASTIC_INDEX_CODE)
            .source(new SearchSourceBuilder()
                .query(boolQueryBuilder)
                .size(0)
                .aggregation(AggregationBuilders.terms("group_by_codescheme")
                    .field("codeScheme.id")
                    .size(1000)
                    .order(BucketOrder.aggregation("best_code_hit", false))
                    .subAggregation(AggregationBuilders.topHits("top_code_hits")
                        .sort(SortBuilders.scoreSort().order(SortOrder.DESC))
                        .size(6)
                        .fetchSource(sourceIncludes))
                    .subAggregation(AggregationBuilders.max("best_code_hit")
                        .script(topHitScript))));
    }

    Map<String, List<DeepSearchHitListDTO<?>>> parseResponse(final SearchResponse response,
                                                             final SearchResultWithMetaDataDTO result,
                                                             final String searchTerm) {
        final Map<String, List<DeepSearchHitListDTO<?>>> ret = new HashMap<>();
        try {
            final Terms groupBy = response.getAggregations().get("group_by_codescheme");
            for (final Terms.Bucket bucket : groupBy.getBuckets()) {
                final TopHits hitsAggr = bucket.getAggregations().get("top_code_hits");
                final SearchHits hits = hitsAggr.getHits();
                long total = hits.getTotalHits();
                if (total > 0) {
                    final String codeSchemeUuid = bucket.getKeyAsString();
                    final List<CodeDTO> topHits = new ArrayList<>();
                    final DeepSearchCodeHitListDTO hitList = new DeepSearchCodeHitListDTO(total, topHits);

                    for (final SearchHit hit : hits.getHits()) {
                        final JsonNode code = objectMapper.readTree(hit.getSourceAsString());
                        final String codeId = ElasticRequestUtils.getTextValueOrNull(code, "id");
                        final String codeUri = ElasticRequestUtils.getTextValueOrNull(code, "uri");
                        final String codeStatus = ElasticRequestUtils.getTextValueOrNull(code, "status");
                        final Map<String, String> prefLabelMap = ElasticRequestUtils.labelFromKeyValueNode(code.get("prefLabel"));
                        final String codeCodeValue = ElasticRequestUtils.getTextValueOrNull(code, "codeValue");

                        final CodeDTO dto = new CodeDTO();
                        dto.setId(UUID.fromString(codeId));
                        dto.setUri(codeUri);
                        dto.setStatus(codeStatus);
                        dto.setPrefLabel(prefLabelMap);
                        dto.setCodeValue(codeCodeValue);
                        addHighlightTagsToDto(searchTerm, dto);
                        final CodeSchemeDTO fat = domain.getCodeScheme(codeSchemeUuid);
                        final CodeSchemeDTO lean = new CodeSchemeDTO();
                        lean.setId(fat.getId());
                        lean.setCodeRegistry(fat.getCodeRegistry());
                        lean.setCodeValue(fat.getCodeValue());
                        dto.setCodeScheme(lean);
                        topHits.add(dto);
                        ret.put(codeSchemeUuid, Collections.singletonList(hitList));

                        final String uuidOfTheCodeScheme = fat.getId().toString().toLowerCase();
                        final Set<String> codeSchemeUuids = new HashSet<>();
                        populateSearchHits(codeSchemeUuids,
                            result,
                            dto.getPrefLabel(),
                            dto.getUri(),
                            dto.getCodeValue(),
                            fat.getCodeValue(),
                            fat.getCodeRegistry().getCodeValue(),
                            uuidOfTheCodeScheme,
                            total);
                    }
                }
            }
        } catch (final Exception e) {
            LOG.error("Cannot parse deep concept query response", e);
        }
        return ret;
    }

    private void addHighlightTagsToDto(final String searchTerm,
                                       final CodeDTO codeDto) {
        highlightLabels(searchTerm, codeDto);
        highlightCodeValue(searchTerm, codeDto);
    }

    private void highlightLabels(final String highlightText,
                                 final CodeDTO codeDto) {
        if (highlightText != null && highlightText.length() > 0) {
            final String[] highLights = highlightText.split("\\s+");
            for (final String highLight : highLights) {
                if (codeDto.getPrefLabel() != null) {
                    codeDto.getPrefLabel().forEach((lang, label) -> {
                        final String matchString = Pattern.quote(highLight);
                        codeDto.getPrefLabel().put(lang, label.replaceAll("(?i)(?<text>\\b" + matchString + "|" + matchString + "\\b)", "<b>${text}</b>"));
                    });
                }
            }
        }
    }

    private void highlightCodeValue(final String highlightText,
                                    final CodeDTO codeDto) {
        if (highlightText != null && highlightText.length() > 0) {
            final String[] highLights = highlightText.split("\\s+");
            for (final String highLight : highLights) {
                final String matchString = Pattern.quote(highLight);
                codeDto.setCodeValue(codeDto.getCodeValue().replaceAll("(?i)(?<text>\\b" + matchString + "|" + matchString + "\\b)", "<b>${text}</b>"));
            }
        }
    }

    private void populateSearchHits(final Set<String> codeSchemeUuids,
                                    final SearchResultWithMetaDataDTO result,
                                    final Map<String, String> prefLabel,
                                    final String uri,
                                    final String entityCodeValue,
                                    final String codeSchemeCodeValue,
                                    final String codeRegistryCodeValue,
                                    final String uuidOfTheCodeScheme,
                                    final long total) {
        codeSchemeUuids.add(uuidOfTheCodeScheme);
        final SearchHitDTO searchHit = new SearchHitDTO();
        searchHit.setType(SEARCH_HIT_TYPE_CODE);
        searchHit.setPrefLabel(prefLabel);
        searchHit.setUri(uri);
        searchHit.setEntityCodeValue(entityCodeValue);
        searchHit.setCodeSchemeCodeValue(codeSchemeCodeValue);
        searchHit.setCodeRegistryCodeValue(codeRegistryCodeValue);

        final Map<String, ArrayList<SearchHitDTO>> searchHits = result.getSearchHitDTOMap();
        if (searchHits.containsKey(uuidOfTheCodeScheme)) {
            final ArrayList<SearchHitDTO> searchHitList = searchHits.get(uuidOfTheCodeScheme);
            searchHitList.add(searchHit);
            searchHits.put(uuidOfTheCodeScheme, searchHitList);
        } else {
            final ArrayList<SearchHitDTO> searchHitList = new ArrayList<>();
            searchHitList.add(searchHit);
            searchHits.put(uuidOfTheCodeScheme, searchHitList);
        }
        result.getSearchHitDTOMap().put(uuidOfTheCodeScheme, searchHits.get(uuidOfTheCodeScheme));
        result.getTotalhitsCodesPerCodeSchemeMap().put(uuidOfTheCodeScheme, total);
    }
}
