package fi.vm.yti.codelist.api;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import fi.vm.yti.codelist.api.domain.Domain;
import fi.vm.yti.codelist.api.exception.YtiCodeListException;
import fi.vm.yti.codelist.common.dto.AbstractIdentifyableCodeDTO;
import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.CodeRegistryDTO;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ErrorModel;
import fi.vm.yti.codelist.common.model.Status;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

abstract public class AbstractTestBase {

    protected static final String TEST_CODEREGISTRY_CODEVALUE = "testregistry1";
    protected static final String TEST_CODESCHEME_CODEVALUE = "testscheme1";
    protected static final String TEST_CODE_CODEVALUE = "testcode1";
    private static final String TEST_BASE_URL = "http://localhost";
    private static final String SOURCE_TEST = "test";
    private static final Logger LOG = LoggerFactory.getLogger(AbstractTestBase.class);
    private static final String MAX_RESULT_WINDOW = "max_result_window";
    private static final int MAX_RESULT_WINDOW_SIZE = 50000;

    private static final String NESTED_PREFLABEL_MAPPING_JSON = "{" +
        "\"properties\": {\n" +
        "  \"codeValue\": {\n" +
        "    \"type\": \"text\"," +
        "    \"analyzer\": \"text_analyzer\",\n" +
        "    \"fields\": {\n" +
        "      \"raw\": { \n" +
        "        \"type\": \"keyword\"\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"id\": {\n" +
        "    \"type\": \"keyword\"\n" +
        "  },\n" +
        "  \"prefLabel\": {\n" +
        "    \"type\": \"nested\"\n" +
        "  }\n" +
        "}\n}";

    private static final String CODESCHEME_MAPPING = "{" +
        "\"dynamic_templates\": [\n" +
        "  {\n" +
        "    \"prefLabel\": {\n" +
        "      \"path_match\": \"prefLabel.*\",\n" +
        "      \"mapping\": {\n" +
        "        \"type\": \"text\",\n" +
        "        \"fields\": {\n" +
        "          \"keyword\": { \n" +
        "            \"type\": \"keyword\",\n" +
        "            \"normalizer\": \"keyword_normalizer\"\n" +
        "          }\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "],\n" +
        "\"properties\": {\n" +
        "  \"codeValue\": {\n" +
        "    \"type\": \"text\"," +
        "    \"analyzer\": \"text_analyzer\",\n" +
        "    \"fields\": {\n" +
        "      \"raw\": { \n" +
        "        \"type\": \"keyword\"\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"id\": {\n" +
        "    \"type\": \"keyword\"" +
        "  },\n" +
        "  \"prefLabel\": {\n" +
        "    \"type\": \"nested\",\n" +
        "    \"dynamic\": true\n" +
        "  },\n" +
        "  \"infoDomains\": {\n" +
        "    \"type\": \"nested\"\n" +
        "  },\n" +
        "  \"codeRegistry\": {\n" +
        "    \"properties\": {\n" +
        "      \"codeValue\": {\n" +
        "        \"type\": \"text\",\n" +
        "        \"analyzer\": \"text_analyzer\"\n" +
        "      },\n" +
        "      \"organizations\": {\n" +
        "        \"type\": \"nested\"\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"extensions\": {\n" +
        "    \"type\": \"nested\"\n" +
        "  },\n" +
        "  \"externalReferences\": {\n" +
        "    \"type\": \"nested\"\n" +
        "  }\n" +
        "}\n}";

    private static final String CODE_MAPPING = "{" +
        "\"properties\": {\n" +
        "  \"codeValue\": {\n" +
        "    \"type\": \"text\"," +
        "    \"analyzer\": \"text_analyzer\",\n" +
        "    \"fields\": {\n" +
        "      \"raw\": { \n" +
        "        \"type\": \"keyword\"\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"id\": {\n" +
        "    \"type\": \"keyword\"\n" +
        "  },\n" +
        "  \"order\": {\n" +
        "    \"type\": \"integer\"\n" +
        "  },\n" +
        "  \"prefLabel\": {\n" +
        "    \"type\": \"nested\"\n" +
        "  },\n" +
        "  \"infoDomains\": {\n" +
        "    \"type\": \"nested\"\n" +
        "  },\n" +
        "  \"codeScheme\": {\n" +
        "    \"properties\": {\n" +
        "      \"codeValue\": {\n" +
        "        \"type\": \"text\",\n" +
        "        \"analyzer\": \"text_analyzer\"\n" +
        "      },\n" +
        "      \"codeRegistry\": {\n" +
        "        \"properties\": {\n" +
        "          \"codeValue\": {\n" +
        "            \"type\": \"text\",\n" +
        "            \"analyzer\": \"text_analyzer\"\n" +
        "          },\n" +
        "          \"organizations\": {\n" +
        "            \"type\": \"nested\"\n" +
        "          }\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"externalReferences\": {\n" +
        "    \"type\": \"nested\"\n" +
        "  }\n" +
        "}\n}";

    @Inject
    private RestHighLevelClient client;

    @Inject
    private Domain domain;

    protected void createAndIndexMockData() {
        createAndIndexMockCodeRegistries();
        createAndIndexMockCodeSchemes(domain.getCodeRegistries());
        createAndIndexMockCodes(domain.getCodeSchemes(null));
        LOG.debug("Mock data indexed!");
    }

    private void createAndIndexMockCodeRegistries() {
        createIndexWithNestedPrefLabel(ELASTIC_INDEX_CODEREGISTRY, ELASTIC_TYPE_CODEREGISTRY);
        final Set<CodeRegistryDTO> codeRegistries = new HashSet<>();
        for (int i = 0; i < 8; i++) {
            codeRegistries.add(createCodeRegistry("testregistry" + (i + 1)));
        }
        indexData(codeRegistries, ELASTIC_INDEX_CODEREGISTRY, ELASTIC_TYPE_CODEREGISTRY);
        refreshIndex(ELASTIC_INDEX_CODEREGISTRY);
        LOG.debug("Indexed " + codeRegistries.size() + " CodeRegistries.");
    }

    private void createAndIndexMockCodeSchemes(final Set<CodeRegistryDTO> codeRegistries) {
        createIndexWithNestedPrefLabel(ELASTIC_INDEX_CODESCHEME, ELASTIC_TYPE_CODESCHEME);
        final Set<CodeSchemeDTO> codeSchemes = new HashSet<>();
        for (final CodeRegistryDTO codeRegistry : codeRegistries) {
            for (int i = 0; i < 8; i++) {
                codeSchemes.add(createCodeScheme(codeRegistry, "testscheme" + (i + 1)));
            }
        }
        indexData(codeSchemes, ELASTIC_INDEX_CODESCHEME, ELASTIC_TYPE_CODESCHEME);
        refreshIndex(ELASTIC_INDEX_CODESCHEME);
        LOG.debug("Indexed " + codeSchemes.size() + " CodeSchemes.");
    }

    private void createAndIndexMockCodes(final Set<CodeSchemeDTO> codeSchemes) {
        createIndexWithNestedPrefLabel(ELASTIC_INDEX_CODE, ELASTIC_TYPE_CODE);
        final Set<CodeDTO> codes = new HashSet<>();
        for (final CodeSchemeDTO codeScheme : codeSchemes) {
            for (int i = 0; i < 8; i++) {
                codes.add(createCode(codeScheme, "testcode" + (i + 1), i));
            }
        }
        indexData(codes, ELASTIC_INDEX_CODE, ELASTIC_TYPE_CODE);
        refreshIndex(ELASTIC_INDEX_CODE);
        LOG.debug("Indexed " + codes.size() + " Codes.");
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

    private void createIndexWithNestedPrefLabel(final String indexName,
                                                final String type) {
        if (!checkIfIndexExists((indexName))) {
            final CreateIndexRequest request = new CreateIndexRequest();
            request.index(indexName);
            try {
                final XContentBuilder contentBuilder = jsonBuilder()
                    .startObject()
                    .startObject("index")
                    .field(MAX_RESULT_WINDOW, MAX_RESULT_WINDOW_SIZE)
                    .endObject()
                    .startObject("analysis")
                    .startObject("analyzer")
                    .startObject("text_analyzer")
                    .field("type", "custom")
                    .field("tokenizer", "keyword")
                    .field("filter", new String[]{ "lowercase", "standard" })
                    .endObject()
                    .endObject()
                    .startObject("normalizer")
                    .startObject("keyword_normalizer")
                    .field("type", "custom")
                    .field("filter", new String[]{ "lowercase" })
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
                request.source(contentBuilder);
            } catch (final IOException e) {
                LOG.error("Error parsing index request settings JSON!", e);
            }
            switch (type) {
                case ELASTIC_TYPE_CODESCHEME:
                    request.mapping(type, CODESCHEME_MAPPING, XContentType.JSON);
                    break;
                case ELASTIC_TYPE_CODE:
                    request.mapping(type, CODE_MAPPING, XContentType.JSON);
                    break;
                default:
                    request.mapping(type, NESTED_PREFLABEL_MAPPING_JSON, XContentType.JSON);
                    break;
            }
            try {
                final CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
                if (!response.isAcknowledged()) {
                    LOG.error("Create failed for index: " + indexName);
                }
            } catch (final IOException e) {
                LOG.error("Error creating index JSON!", e);
            }
        }
    }

    private <T> boolean indexData(final Set<T> set,
                                  final String elasticIndex,
                                  final String elasticType) {
        boolean success = true;
        if (!set.isEmpty()) {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
            mapper.setFilterProvider(new SimpleFilterProvider().setFailOnUnknownId(false));
            final BulkRequest bulkRequest = new BulkRequest();
            for (final T item : set) {
                try {
                    final AbstractIdentifyableCodeDTO identifyableCode = (AbstractIdentifyableCodeDTO) item;
                    final String itemPayload = mapper.writeValueAsString(item).replace("\\\\n", "\\n");
                    bulkRequest.add(new IndexRequest(elasticIndex, elasticType, identifyableCode.getId().toString()).source(itemPayload, XContentType.JSON));
                    bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
                } catch (JsonProcessingException e) {
                    LOG.error("Error happened during indexing", e);
                }
            }
            try {
                final BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                if (response.hasFailures()) {
                    LOG.error("Bulk indexing response failed: " + response.buildFailureMessage());
                    success = false;
                }
            } catch (final IOException e) {
                LOG.error("Bulk index request failed!", e);
            }
        } else {
            LOG.error("Trying to index empty dataset..");
            success = false;
        }
        return success;
    }

    @SuppressFBWarnings("RR_NOT_CHECKED")
    private void refreshIndex(final String indexName) {
        final FlushRequest request = new FlushRequest(indexName);
        try {
            client.indices().flush(request, RequestOptions.DEFAULT);
            LOG.debug("Index flushed successfully: " + indexName);
        } catch (final IOException e) {
            LOG.error("Index flush failed for index: " + indexName, e);
        }
    }

    private CodeRegistryDTO createCodeRegistry(final String codeValue) {
        final CodeRegistryDTO codeRegistry = new CodeRegistryDTO();
        codeRegistry.setId(UUID.randomUUID());
        codeRegistry.setCodeValue(codeValue);
        codeRegistry.setPrefLabel(LANGUAGE_CODE_FI, "Testirekisteri");
        codeRegistry.setPrefLabel(LANGUAGE_CODE_SV, "Test register");
        codeRegistry.setPrefLabel(LANGUAGE_CODE_EN, "Test register");
        codeRegistry.setDescription(LANGUAGE_CODE_FI, "Testi määritelmä");
        codeRegistry.setDescription(LANGUAGE_CODE_SV, "Test upplösning");
        codeRegistry.setDescription(LANGUAGE_CODE_EN, "Test definition");
        codeRegistry.setModified(new Date(System.currentTimeMillis()));
        codeRegistry.setUrl("http://localhost:9601/codelist-api/api/v1/coderegistries/" + codeValue + "/");
        return codeRegistry;
    }

    private CodeSchemeDTO createCodeScheme(final CodeRegistryDTO codeRegistry,
                                           final String codeValue) {
        final CodeSchemeDTO codeScheme = new CodeSchemeDTO();
        codeScheme.setId(UUID.randomUUID());
        codeScheme.setCodeValue(codeValue);
        codeScheme.setStatus(Status.VALID.toString());
        codeScheme.setPrefLabel(LANGUAGE_CODE_FI, "Testikoodisto");
        codeScheme.setPrefLabel(LANGUAGE_CODE_SV, "Test kodlist");
        codeScheme.setPrefLabel(LANGUAGE_CODE_EN, "Test scheme");
        codeScheme.setDefinition(LANGUAGE_CODE_FI, "Testi määritelmä");
        codeScheme.setDefinition(LANGUAGE_CODE_SV, "Test upplösning");
        codeScheme.setDefinition(LANGUAGE_CODE_EN, "Test definition");
        codeScheme.setUrl("http://localhost:9601/codelist-api/api/v1/coderegistries/" + codeRegistry.getCodeValue() + "/codeschemes/" + codeScheme.getCodeValue() + "/");
        codeScheme.setCodeRegistry(codeRegistry);
        codeScheme.setSource(SOURCE_TEST);
        codeScheme.setModified(new Date(System.currentTimeMillis()));
        return codeScheme;
    }

    private CodeDTO createCode(final CodeSchemeDTO codeScheme,
                               final String codeValue,
                               final int order) {
        final CodeDTO code = new CodeDTO();
        code.setId(UUID.randomUUID());
        code.setCodeValue(codeValue);
        code.setStatus(Status.VALID.toString());
        code.setPrefLabel(LANGUAGE_CODE_FI, "Testikoodi");
        code.setPrefLabel(LANGUAGE_CODE_SV, "Test kod");
        code.setPrefLabel(LANGUAGE_CODE_EN, "Test code");
        code.setDefinition(LANGUAGE_CODE_FI, "Testi määritelmä");
        code.setDefinition(LANGUAGE_CODE_SV, "Test upplösning");
        code.setDefinition(LANGUAGE_CODE_EN, "Test definition");
        code.setDescription(LANGUAGE_CODE_FI, "Testi kuvaus");
        code.setDefinition(LANGUAGE_CODE_SV, "Test beskrivning");
        code.setDefinition(LANGUAGE_CODE_EN, "Test description");
        code.setOrder(order);
        code.setShortName("ABR");
        code.setCodeScheme(codeScheme);
        code.setModified(new Date(System.currentTimeMillis()));
        code.setUrl("http://localhost:9601/codelist-api/api/v1/coderegistries/" + codeScheme.getCodeRegistry().getCodeValue() + "/codeschemes/" + codeScheme.getCodeValue() + "/codes/" + code.getCodeValue() + "/");
        return code;
    }

    protected String createApiUrlWithoutVersion(final int serverPort,
                                                final String apiPath) {
        return TEST_BASE_URL + ":" + serverPort + API_CONTEXT_PATH_RESTAPI + API_BASE_PATH + apiPath + "/";
    }

    protected String createApiUrl(final int serverPort,
                                  final String apiPath) {
        return TEST_BASE_URL + ":" + serverPort + API_CONTEXT_PATH_RESTAPI + API_BASE_PATH + API_PATH_VERSION_V1 + apiPath + "/";
    }
}
