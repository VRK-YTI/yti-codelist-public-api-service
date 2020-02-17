package fi.vm.yti.codelist.api.integration;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import fi.vm.yti.codelist.api.AbstractTestBase;
import fi.vm.yti.codelist.api.PublicApiServiceApplication;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {PublicApiServiceApplication.class})
@ActiveProfiles({"automatedtest"})
@TestPropertySource(locations = "classpath:test-port.properties")
public class PopulateElasticTestDataT1 extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceIntegrationT2.class);

    @Test
    public void populateData() {
        LOG.debug("Indexing mock data to data ElasticSearch.");
        Assert.assertTrue(createAndIndexMockData());
        LOG.debug("Indexing done.");
    }
}
