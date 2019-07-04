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

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
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

import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.DeepSearchExtensionHitListDTO;
import fi.vm.yti.codelist.common.dto.DeepSearchHitListDTO;
import fi.vm.yti.codelist.common.dto.ExtensionDTO;
import fi.vm.yti.codelist.common.dto.SearchHitDTO;
import fi.vm.yti.codelist.common.dto.SearchResultWithMetaDataDTO;
import static fi.vm.yti.codelist.common.constants.ApiConstants.SEARCH_HIT_TYPE_EXTENSION;
import static org.elasticsearch.index.query.QueryBuilders.*;

class DeepExtensionQueryFactory {

    private static final Logger log = LoggerFactory.getLogger(DeepExtensionQueryFactory.class);

    private static final FetchSourceContext sourceIncludes = new FetchSourceContext(true, new String[]{ "id", "codeValue", "prefLabel", "parentCodeScheme.id" }, new String[]{});
    private static final Script topHitScript = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "_score", Collections.emptyMap());
    private final Domain domain;
    private ObjectMapper objectMapper;

    DeepExtensionQueryFactory(final ObjectMapper objectMapper,
                              final Domain domain) {
        this.objectMapper = objectMapper;
        this.domain = domain;
    }

    SearchRequest createQuery(final String query,
                              final String extensionPropertyType) {

        final BoolQueryBuilder boolQueryBuilder = boolQuery();
        if (query != null && !query.isEmpty()) {
            boolQueryBuilder.should(prefixQuery("codeValue",
                query.toLowerCase()));
            boolQueryBuilder.should(nestedQuery("prefLabel",
                multiMatchQuery(query.toLowerCase() + "*",
                    "prefLabel.*").type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX),
                ScoreMode.None));
        }
        boolQueryBuilder.minimumShouldMatch(1);

        if (extensionPropertyType != null) {
            boolQueryBuilder.must(matchQuery("propertyType.localName", extensionPropertyType));
        }

        return new SearchRequest("extension")
            .source(new SearchSourceBuilder()
                .query(boolQueryBuilder)
                .size(0)
                .aggregation(AggregationBuilders.terms("group_by_codescheme")
                    .field("parentCodeScheme.id")
                    .size(1000)
                    .order(BucketOrder.aggregation("best_extension_hit", false))
                    .subAggregation(AggregationBuilders.topHits("top_extension_hits")
                        .sort(SortBuilders.scoreSort().order(SortOrder.DESC))
                        .size(6)
                        .fetchSource(sourceIncludes)
                    ).subAggregation(AggregationBuilders.max("best_extension_hit")
                        .script(topHitScript))));
    }

    public Map<String, List<DeepSearchHitListDTO<?>>> parseResponse(SearchResponse response,
                                                                    SearchResultWithMetaDataDTO result,
                                                                    String searchTerm) {
        Map<String, List<DeepSearchHitListDTO<?>>> ret = new HashMap<>();
        try {
            Terms groupBy = response.getAggregations().get("group_by_codescheme");
            for (Terms.Bucket bucket : groupBy.getBuckets()) {
                TopHits hitsAggr = bucket.getAggregations().get("top_extension_hits");
                SearchHits hits = hitsAggr.getHits();

                long total = hits.getTotalHits();
                if (total > 0) {
                    String codeSchemeUuid = bucket.getKeyAsString();
                    List<ExtensionDTO> topHits = new ArrayList<>();
                    DeepSearchExtensionHitListDTO hitList = new DeepSearchExtensionHitListDTO(total, topHits);

                    for (SearchHit hit : hits.getHits()) {
                        JsonNode code = objectMapper.readTree(hit.getSourceAsString());
                        String codeId = ElasticRequestUtils.getTextValueOrNull(code, "id");
                        Map<String, String> prefLabelMap = ElasticRequestUtils.labelFromKeyValueNode(code.get("prefLabel"));
                        String codeCodeValue = ElasticRequestUtils.getTextValueOrNull(code, "codeValue");
                        ExtensionDTO dto = new ExtensionDTO();
                        dto.setId(UUID.fromString(codeId));
                        dto.setPrefLabel(prefLabelMap);
                        dto.setCodeValue(codeCodeValue);
                        addHighlightTagsToDto(searchTerm, dto);
                        CodeSchemeDTO fat = domain.getCodeScheme(codeSchemeUuid);
                        CodeSchemeDTO lean = new CodeSchemeDTO();
                        lean.setId(fat.getId());
                        lean.setCodeRegistry(fat.getCodeRegistry());
                        lean.setCodeValue(fat.getCodeValue());
                        dto.setParentCodeScheme(lean);
                        topHits.add(dto);
                        ret.put(codeSchemeUuid, Collections.singletonList(hitList));

                        String uuidOfTheCodeScheme = fat.getId().toString().toLowerCase();
                        final Set<String> codeSchemeUuids = new HashSet<>();
                        populateSearchHits(codeSchemeUuids,
                            result,
                            dto.getPrefLabel(),
                            dto.getCodeValue(),
                            fat.getCodeValue(),
                            fat.getCodeRegistry().getCodeValue(),
                            uuidOfTheCodeScheme,
                            total);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Cannot parse deep concept query response", e);
        }
        return ret;
    }

    private void addHighlightTagsToDto(String searchTerm,
                                       ExtensionDTO dto) {
        highlightLabels(searchTerm, dto);
        highlightCodeValue(searchTerm, dto);
    }

    private void highlightLabels(String highlightText,
                                 ExtensionDTO dto) {
        if (highlightText != null && highlightText.length() > 0) {
            final String[] highLights = highlightText.split("\\s+");
            for (final String highLight : highLights) {
                if (dto.getPrefLabel() != null) {
                    dto.getPrefLabel().forEach((lang, label) -> {
                        String matchString = Pattern.quote(highLight);
                        dto.getPrefLabel().put(lang, label.replaceAll("(?i)(?<text>\\b" + matchString + "|" + matchString + "\\b)", "<b>${text}</b>"));
                    });
                }
            }
        }
    }

    private void highlightCodeValue(String highlightText,
                                    ExtensionDTO dto) {
        if (highlightText != null && highlightText.length() > 0) {
            final String[] highLights = highlightText.split("\\s+");
            for (final String highLight : highLights) {
                String matchString = Pattern.quote(highLight);
                dto.setCodeValue(dto.getCodeValue().replaceAll("(?i)(?<text>\\b" + matchString + "|" + matchString + "\\b)", "<b>${text}</b>"));
            }
        }
    }

    private void populateSearchHits(final Set<String> codeSchemeUuids,
                                    final SearchResultWithMetaDataDTO result,
                                    final Map<String, String> prefLabel,
                                    final String entityCodeValue,
                                    final String codeSchemeCodeValue,
                                    final String codeRegistryCodeValue,
                                    final String uuidOfTheCodeScheme,
                                    final long total) {
        codeSchemeUuids.add(uuidOfTheCodeScheme);
        SearchHitDTO searchHit = new SearchHitDTO();
        searchHit.setType(SEARCH_HIT_TYPE_EXTENSION);
        searchHit.setPrefLabel(prefLabel);
        searchHit.setEntityCodeValue(entityCodeValue);
        searchHit.setCodeSchemeCodeValue(codeSchemeCodeValue);
        searchHit.setCodeRegistryCodeValue(codeRegistryCodeValue);

        Map<String, ArrayList<SearchHitDTO>> searchHits = result.getSearchHitDTOMap();
        if (searchHits.containsKey(uuidOfTheCodeScheme)) {
            ArrayList<SearchHitDTO> searchHitList = searchHits.get(uuidOfTheCodeScheme);
            searchHitList.add(searchHit);
            searchHits.put(uuidOfTheCodeScheme, searchHitList);
        } else {
            ArrayList<SearchHitDTO> searchHitList = new ArrayList<>();
            searchHitList.add(searchHit);
            searchHits.put(uuidOfTheCodeScheme, searchHitList);
        }
        result.getSearchHitDTOMap().put(uuidOfTheCodeScheme,
            searchHits.get(uuidOfTheCodeScheme));
        result.getTotalhitsExtensionsPerCodeSchemeMap().put(uuidOfTheCodeScheme, total);
    }
}
