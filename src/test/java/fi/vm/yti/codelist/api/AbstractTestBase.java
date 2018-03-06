package fi.vm.yti.codelist.api;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import fi.vm.yti.codelist.api.domain.Domain;
import fi.vm.yti.codelist.common.model.Code;
import fi.vm.yti.codelist.common.model.CodeRegistry;
import fi.vm.yti.codelist.common.model.CodeScheme;
import fi.vm.yti.codelist.common.model.Status;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

abstract public class AbstractTestBase {

    public static final String TEST_BASE_URL = "http://localhost";
    public static final String TEST_CODEREGISTRY_CODEVALUE = "testregistry1";
    public static final String TEST_CODESCHEME_CODEVALUE = "testscheme1";
    public static final String TEST_CODE_CODEVALUE = "testcode1";

    private static final String SOURCE_TEST = "test";
    private static final Logger LOG = LoggerFactory.getLogger(AbstractTestBase.class);
    private static final String MAX_RESULT_WINDOW = "max_result_window";
    private static final int MAX_RESULT_WINDOW_SIZE = 500000;

    private static final String NESTED_PREFLABEL_MAPPING_JSON = "{" +
        "\"properties\": {\n" +
        "  \"prefLabel\": {\n" +
        "    \"type\": \"nested\"\n" +
        "  }\n" +
        "}\n}";

    @Inject
    private Client client;

    @Inject
    private Domain domain;

    private CodeScheme createCodeScheme(final CodeRegistry codeRegistry,
                                        final String codeValue) {
        final CodeScheme codeScheme = new CodeScheme();
        codeScheme.setId(UUID.randomUUID());
        codeScheme.setCodeValue(codeValue);
        codeScheme.setStatus(Status.VALID.toString());
        codeScheme.setPrefLabel(LANGUAGE_CODE_FI, "Testikoodisto");
        codeScheme.setPrefLabel(LANGUAGE_CODE_SV, "Test kodlist");
        codeScheme.setPrefLabel(LANGUAGE_CODE_EN, "Test scheme");
        codeScheme.setDefinition(LANGUAGE_CODE_FI, "Testi määritelmä");
        codeScheme.setDefinition(LANGUAGE_CODE_SV, "Test upplösning");
        codeScheme.setDefinition(LANGUAGE_CODE_EN, "Test definition");
        codeScheme.setUri("http://localhost:9601/codelist-api/api/v1/coderegistries/" + codeRegistry.getCodeValue() + "/codeschemes/" + codeScheme.getCodeValue() + "/");
        codeScheme.setCodeRegistry(codeRegistry);
        codeScheme.setSource(SOURCE_TEST);
        codeScheme.setModified(new Date(System.currentTimeMillis()));
        return codeScheme;
    }

    public void createAndIndexMockData() {
        createAndIndexMockCodeRegistries();
        createAndIndexMockCodeSchemes(domain.getCodeRegistries());
        createAndIndexMockCodes(domain.getCodeSchemes());
        LOG.info("Mock data indexed!");
    }

    private void createAndIndexMockCodeRegistries() {
        createIndexWithNestedPrefLabel(ELASTIC_INDEX_CODEREGISTRY, ELASTIC_TYPE_CODEREGISTRY);
        final Set<CodeRegistry> codeRegistries = new HashSet<>();
        for (int i = 0; i < 8; i++) {
            codeRegistries.add(createCodeRegistry("testregistry" + (i + 1)));
        }
        indexData(codeRegistries, ELASTIC_INDEX_CODEREGISTRY, ELASTIC_TYPE_CODEREGISTRY);
        refreshIndex(ELASTIC_INDEX_CODEREGISTRY);
        LOG.info("Indexed " + codeRegistries.size() + " CodeRegistries.");
    }

    private void createAndIndexMockCodeSchemes(final Set<CodeRegistry> codeRegistries) {
        createIndexWithNestedPrefLabel(ELASTIC_INDEX_CODESCHEME, ELASTIC_TYPE_CODESCHEME);
        final Set<CodeScheme> codeSchemes = new HashSet<>();
        for (final CodeRegistry codeRegistry : codeRegistries) {
            for (int i = 0; i < 8; i++) {
                codeSchemes.add(createCodeScheme(codeRegistry, "testscheme" + (i + 1)));
            }
        }
        indexData(codeSchemes, ELASTIC_INDEX_CODESCHEME, ELASTIC_TYPE_CODESCHEME);
        refreshIndex(ELASTIC_INDEX_CODESCHEME);
        LOG.info("Indexed " + codeSchemes.size() + " CodeSchemes.");
    }

    private void createAndIndexMockCodes(final Set<CodeScheme> codeSchemes) {
        createIndexWithNestedPrefLabel(ELASTIC_INDEX_CODE, ELASTIC_TYPE_CODE);
        final Set<Code> codes = new HashSet<>();
        for (final CodeScheme codeScheme : codeSchemes) {
            for (int i = 0; i < 8; i++) {
                codes.add(createCode(codeScheme, "testcode" + (i + 1)));
            }
        }
        indexData(codes, ELASTIC_INDEX_CODE, ELASTIC_TYPE_CODE);
        refreshIndex(ELASTIC_INDEX_CODE);
        LOG.info("Indexed " + codes.size() + " Codes.");
    }

    private void createIndexWithNestedPrefLabel(final String indexName, final String type) {
        final boolean exists = client.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
        if (!exists) {
            final CreateIndexRequestBuilder builder = client.admin().indices().prepareCreate(indexName);
            builder.setSettings(Settings.builder().put(MAX_RESULT_WINDOW, MAX_RESULT_WINDOW_SIZE));
            builder.addMapping(type, NESTED_PREFLABEL_MAPPING_JSON, XContentType.JSON);
            final CreateIndexResponse response = builder.get();
            if (!response.isAcknowledged()) {
                LOG.error("Create failed for index: " + indexName);
            }
        }
    }

    private <T> boolean indexData(final Set<T> set,
                                  final String elasticIndex,
                                  final String elasticType) {
        boolean success = true;
        if (!set.isEmpty()) {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.setFilterProvider(new SimpleFilterProvider().setFailOnUnknownId(false));
            final BulkRequestBuilder bulkRequest = client.prepareBulk();
            for (final T item : set) {
                try {
                    bulkRequest.add(client.prepareIndex(elasticIndex, elasticType).setSource(mapper.writeValueAsString(item), XContentType.JSON));
                } catch (JsonProcessingException e) {
                    LOG.error("Error happened during indexing", e);
                }
            }
            final BulkResponse response = bulkRequest.get();
            if (response.hasFailures()) {
                LOG.error("Bulk indexing response failed: " + response.buildFailureMessage());
                success = false;
            }
        } else {
            LOG.error("Trying to index empty dataset..");
            success = false;
        }
        return success;
    }

    /**
     * Refreshes index with name.
     *
     * @param indexName The name of the index to be refreshed.
     */
    @SuppressFBWarnings("RR_NOT_CHECKED")
    public void refreshIndex(final String indexName) {
        final FlushRequest request = new FlushRequest(indexName);
        try {
            client.admin().indices().flush(request).get();
            LOG.info("Index flushed successfully: " + indexName);
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Index flush failed for index: " + indexName, e);
        }
    }

    private CodeRegistry createCodeRegistry(final String codeValue) {
        final CodeRegistry codeRegistry = new CodeRegistry();
        codeRegistry.setId(UUID.randomUUID());
        codeRegistry.setCodeValue(codeValue);
        codeRegistry.setPrefLabel(LANGUAGE_CODE_FI, "Testirekisteri");
        codeRegistry.setPrefLabel(LANGUAGE_CODE_SV, "Test register");
        codeRegistry.setPrefLabel(LANGUAGE_CODE_EN, "Test register");
        codeRegistry.setDefinition(LANGUAGE_CODE_FI, "Testi määritelmä");
        codeRegistry.setDefinition(LANGUAGE_CODE_SV, "Test upplösning");
        codeRegistry.setDefinition(LANGUAGE_CODE_EN, "Test definition");
        codeRegistry.setModified(new Date(System.currentTimeMillis()));
        codeRegistry.setUri("http://localhost:9601/codelist-api/api/v1/coderegistries/" + codeValue + "/");
        return codeRegistry;
    }

    private Code createCode(final CodeScheme codeScheme,
                            final String codeValue) {
        final Code code = new Code();
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
        code.setShortName("ABR");
        code.setCodeScheme(codeScheme);
        code.setModified(new Date(System.currentTimeMillis()));
        code.setUri("http://localhost:9601/codelist-api/api/v1/coderegistries/" + codeScheme.getCodeRegistry().getCodeValue() + "/codeschemes/" + codeScheme.getCodeValue() + "/codes/" + code.getCodeValue() + "/");
        return code;
    }

    public String createApiUrlWithoutVersion(final int serverPort,
                                             final String apiPath) {
        return TEST_BASE_URL + ":" + serverPort + API_CONTEXT_PATH_RESTAPI + API_BASE_PATH + apiPath + "/";
    }

    public String createApiUrl(final int serverPort,
                               final String apiPath) {
        return TEST_BASE_URL + ":" + serverPort + API_CONTEXT_PATH_RESTAPI + API_BASE_PATH + API_PATH_VERSION_V1 + apiPath + "/";
    }
}
