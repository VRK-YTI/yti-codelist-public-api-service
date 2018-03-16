package fi.vm.yti.codelist.api.util;

import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;


public abstract class FileUtils {

    private FileUtils() {
    }

    /**
     * Loads a file from classpath inside the application JAR.
     *
     * @param fileName The name of the file to be loaded.
     */
    public static InputStream loadFileFromClassPath(final String fileName) throws IOException {

        final ClassPathResource classPathResource = new ClassPathResource(fileName);
        return classPathResource.getInputStream();
    }
}
