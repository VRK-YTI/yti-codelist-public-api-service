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

import javax.ws.rs.BadRequestException;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
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
import static fi.vm.yti.codelist.common.constants.ApiConstants.SEARCH_HIT_TYPE_CODE;
import static org.elasticsearch.index.query.QueryBuilders.*;

public class DeepCodeQueryFactory {

    private static final Logger log = LoggerFactory.getLogger(DeepCodeQueryFactory.class);

    private static final Pattern prefLangPattern = Pattern.compile("[a-zA-Z-]+");
    private static final FetchSourceContext sourceIncludes = new FetchSourceContext(true, new String[]{ "id", "uri", "status", "codeValue", "prefLabel", "codeScheme.id" }, new String[]{});
    private static final Script topHitScript = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "_score", Collections.emptyMap());
    private final Domain domain;
    private ObjectMapper objectMapper;

    public DeepCodeQueryFactory(final ObjectMapper objectMapper,
                                final Domain domain) {
        this.objectMapper = objectMapper;
        this.domain = domain;
    }

    public QueryStringQueryBuilder buildPrefixSuffixQuery(String query) {
        String parsedQuery = null;

        if (!query.contains("*")) {
            if (query.contains(" ")) {
                String[] splittedQry = query.split("\\s+");
                for (int i = 0; i < splittedQry.length; i++) {
                    splittedQry[i] = ("(" + splittedQry[i] + " OR *" + splittedQry[i] + " OR " + splittedQry[i] + "*)");
                }
                parsedQuery = String.join(" AND ", splittedQry);
            } else {
                parsedQuery = query + " OR " + query + "* OR *" + query;
            }
        } else {
            parsedQuery = query;
        }

        StandardQueryParser parser = new StandardQueryParser();
        Query luceneQuery = null;

        try {
            parser.setAllowLeadingWildcard(true);
            luceneQuery = parser.parse(parsedQuery, "");
        } catch (QueryNodeException e) {
            log.warn("Failed to parse: " + parsedQuery);
            throw new BadRequestException("Invalid query!");
        }

        return QueryBuilders.queryStringQuery(luceneQuery.toString());
    }

    public SearchRequest createQuery(String query,
                                     String prefLang) {


        // QueryStringQueryBuilder queryStringQuery = this.buildPrefixSuffixQuery(query).field("prefLabel.*"); // TODO YTI-421 clean up this stuff or make it work
        // QueryStringQueryBuilder queryStringQuery = this.buildPrefixSuffixQuery(query.toLowerCase()).field("prefLabel.*");

        String[] fieldNames = { "prefLabel.*", "codeValue" };
        MultiMatchQueryBuilder multiMatch = QueryBuilders.multiMatchQuery(query, fieldNames)
            .type(MultiMatchQueryBuilder.Type.BEST_FIELDS)
            .minimumShouldMatch("90%");

        final BoolQueryBuilder boolQueryBuilder = boolQuery();
        if (query != null && !query.isEmpty()) {
            boolQueryBuilder.should(prefixQuery("codeValue",
                query.toLowerCase()));
            boolQueryBuilder.should(nestedQuery("prefLabel",
                multiMatchQuery(query.toLowerCase() + "*",
                    "prefLabel.*").type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX),
                ScoreMode.None));

            /*boolQueryBuilder.should(prefixQuery("codeValue", // TODO YTI-421 clean up this stuff or make it work
                query.toLowerCase()));
            boolQueryBuilder.should(queryStringQuery);*/

        }
        boolQueryBuilder.minimumShouldMatch(1);

        if (prefLang != null && prefLangPattern.matcher(prefLang).matches()) {
            multiMatch = multiMatch.field("prefLabel." + prefLang, 10);
        }

        SearchRequest sr = new SearchRequest("code")
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
                        .fetchSource(sourceIncludes)
                    ).subAggregation(AggregationBuilders.max("best_code_hit")
                        .script(topHitScript))));
        return sr;
    }

    public Map<String, List<DeepSearchHitListDTO<?>>> parseResponse(SearchResponse response,
                                                                    SearchResultWithMetaDataDTO result,
                                                                    String searchTerm) {
        Map<String, List<DeepSearchHitListDTO<?>>> ret = new HashMap<>();
        try {
            Terms groupBy = response.getAggregations().get("group_by_codescheme");
            for (Terms.Bucket bucket : groupBy.getBuckets()) {
                TopHits hitsAggr = bucket.getAggregations().get("top_code_hits");
                SearchHits hits = hitsAggr.getHits();

                long total = hits.getTotalHits();
                if (total > 0) {
                    String codeSchemeUuid = bucket.getKeyAsString();
                    List<CodeDTO> topHits = new ArrayList<>();
                    DeepSearchCodeHitListDTO hitList = new DeepSearchCodeHitListDTO(total, topHits);

                    for (SearchHit hit : hits.getHits()) {
                        JsonNode code = objectMapper.readTree(hit.getSourceAsString());
                        String codeId = ElasticRequestUtils.getTextValueOrNull(code, "id");
                        String codeUri = ElasticRequestUtils.getTextValueOrNull(code, "uri");
                        String codeStatus = ElasticRequestUtils.getTextValueOrNull(code, "status");
                        Map<String, String> prefLabelMap = ElasticRequestUtils.labelFromKeyValueNode(code.get("prefLabel"));
                        String codeCodeValue = ElasticRequestUtils.getTextValueOrNull(code, "codeValue");
                        String codeCodeSchemeUuid = ElasticRequestUtils.getTextValueOrNull(code, "codeScheme.id");

                        CodeDTO dto = new CodeDTO();
                        dto.setId(UUID.fromString(codeId));
                        dto.setUri(codeUri);
                        dto.setStatus(codeStatus);
                        dto.setPrefLabel(prefLabelMap);
                        dto.setCodeValue(codeCodeValue);
                        addHighlightTagsToDto(searchTerm, dto);
                        CodeSchemeDTO fat = domain.getCodeScheme(codeSchemeUuid);
                        CodeSchemeDTO lean = new CodeSchemeDTO();
                        lean.setId(fat.getId());
                        lean.setCodeRegistry(fat.getCodeRegistry());
                        lean.setCodeValue(fat.getCodeValue());
                        dto.setCodeScheme(lean);
                        topHits.add(dto);
                        ret.put(codeSchemeUuid, Collections.singletonList(hitList));

                        String uuidOfTheCodeScheme = fat.getId().toString().toLowerCase();
                        final Set<String> codeSchemeUuids = new HashSet<>();
                        populateSearchHits(codeSchemeUuids,
                            result,
                            dto.getPrefLabel(),
                            dto.getUri(),
                            dto.getCodeValue(),
                            fat.getCodeValue(),
                            fat.getCodeRegistry().getCodeValue(),
                            uuidOfTheCodeScheme,
                            SEARCH_HIT_TYPE_CODE,
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
                                       CodeDTO dto) {
        highlightLabels(searchTerm, dto);
        highlightCodeValue(searchTerm, dto);
    }

    private void highlightLabels(String highlightText,
                                 CodeDTO dto) {
        if (highlightText != null && highlightText.length() > 0) {
            String highLights[] = highlightText.split("\\s+");
            for (String highLight : highLights) {
                dto.getPrefLabel().forEach((lang, label) -> {
                    String matchString = Pattern.quote(highLight);
                    dto.getPrefLabel().put(lang, label.replaceAll("(?i)(?<text>\\b" + matchString + "|" + matchString + "\\b)", "<b>${text}</b>"));
                });
            }
        }
    }

    private void highlightCodeValue(String highlightText,
                                    CodeDTO dto) {
        if (highlightText != null && highlightText.length() > 0) {
            String highLights[] = highlightText.split("\\s+");
            for (String highLight : highLights) {
                String matchString = Pattern.quote(highLight);
                dto.setCodeValue(dto.getCodeValue().replaceAll("(?i)(?<text>\\b" + matchString + "|" + matchString + "\\b)", "<b>${text}</b>"));
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
                                    final String typeOfHit,
                                    final long total) {
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
        result.getTotalhitsCodesPerCodeSchemeMap().put(uuidOfTheCodeScheme, new Long(total));
    }
}
