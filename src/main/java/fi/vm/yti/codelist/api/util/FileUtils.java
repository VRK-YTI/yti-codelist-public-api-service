package fi.vm.yti.codelist.api.util;

import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;

public interface FileUtils {

    static InputStream loadFileFromClassPath(final String fileName) throws IOException {

        final ClassPathResource classPathResource = new ClassPathResource(fileName);
        return classPathResource.getInputStream();
    }
}
