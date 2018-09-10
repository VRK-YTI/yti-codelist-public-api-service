package fi.vm.yti.codelist.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.ApplicationContext;

@SpringBootApplication(scanBasePackages = "fi.vm.yti.codelist.api", exclude = { DataSourceAutoConfiguration.class, HibernateJpaAutoConfiguration.class })
public class PublicApiServiceApplication {

    public static void main(final String[] args) {
        final ApplicationContext context = SpringApplication.run(PublicApiServiceApplication.class, args);
        final AppInitializer serviceInitializer = context.getBean(AppInitializer.class);
        serviceInitializer.initialize();
    }
}
