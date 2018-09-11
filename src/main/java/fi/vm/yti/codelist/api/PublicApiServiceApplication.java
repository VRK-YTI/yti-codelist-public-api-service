package fi.vm.yti.codelist.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;

@SpringBootApplication(scanBasePackages = "fi.vm.yti.codelist.api", exclude = { DataSourceAutoConfiguration.class, HibernateJpaAutoConfiguration.class })
public class PublicApiServiceApplication {

    public static void main(final String[] args) {
        SpringApplication.run(PublicApiServiceApplication.class, args);
    }
}
