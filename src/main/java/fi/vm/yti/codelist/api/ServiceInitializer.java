package fi.vm.yti.codelist.api;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.api.configuration.VersionInformation;

@Component
public class ServiceInitializer implements ApplicationRunner {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceInitializer.class);
    private final VersionInformation versionInformation;

    @Inject
    public ServiceInitializer(final VersionInformation versionInformation) {
        this.versionInformation = versionInformation;
    }

    @Override
    public void run(final ApplicationArguments applicationArguments) {
        initialize();
    }

    private void initialize() {
        printLogo();
    }

    private void printLogo() {
        LOG.info("          __  .__                      ___.   .__  .__        ");
        LOG.info(" ___.__._/  |_|__|         ______  __ _\\_ |__ |  | |__| ____  ");
        LOG.info("<   |  |\\   __\\  |  ______ \\____ \\|  |  \\ __ \\|  | |  |/ ___\\ ");
        LOG.info(" \\___  | |  | |  | /_____/ |  |_> >  |  / \\_\\ \\  |_|  \\  \\___ ");
        LOG.info(" / ____| |__| |__|         |   __/|____/|___  /____/__|\\___  >");
        LOG.info(" \\/                        |__|             \\/             \\/ ");
        LOG.info("              .__                            .__              ");
        LOG.info("_____  ______ |__|   ______ ______________  _|__| ____  ____  ");
        LOG.info("\\__  \\ \\____ \\|  |  /  ___// __ \\_  __ \\  \\/ /  |/ ___\\/ __ \\ ");
        LOG.info(" / __ \\|  |_> >  |  \\___ \\\\  ___/|  | \\/\\   /|  \\  \\__\\  ___/ ");
        LOG.info("(____  /   __/|__| /____  >\\___  >__|    \\_/ |__|\\___  >___  >");
        LOG.info("     \\/|__|             \\/     \\/                    \\/    \\/ ");
        LOG.info("");
        LOG.info("                --- Version " + versionInformation.getVersion() + " starting up. --- ");
        LOG.info("");
    }
}
