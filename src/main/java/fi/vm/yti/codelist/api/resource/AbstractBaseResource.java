package fi.vm.yti.codelist.api.resource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.jaxrs.cfg.EndpointConfigBase;
import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterModifier;

import fi.vm.yti.codelist.api.api.ErrorWrapper;
import fi.vm.yti.codelist.common.model.Meta;
import fi.vm.yti.codelist.common.model.Status;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

abstract class AbstractBaseResource {

    public static final String DOWNLOAD_FILENAME_CODEREGISTRIES = "coderegistries";
    public static final String DOWNLOAD_FILENAME_CODESCHEMES = "codeschemes";
    public static final String DOWNLOAD_FILENAME_CODES = "codes";
    public static final String DOWNLOAD_FILENAME_EXTERNALREFERENCES = "externalreferences";
    public static final String DOWNLOAD_FILENAME_PROPERTYTYPES = "propertytypes";

    public static final String HEADER_CONTENT_DISPOSITION = "content-disposition";

    public SimpleFilterProvider createSimpleFilterProvider(final String baseFilter,
                                                           final String expand) {
        final List<String> baseFilters = new ArrayList<>();
        baseFilters.add(baseFilter);
        return createSimpleFilterProvider(baseFilters, expand);
    }

    public SimpleFilterProvider createSimpleFilterProvider(final List<String> baseFilters,
                                                           final String expand) {
        final SimpleFilterProvider filterProvider = new SimpleFilterProvider();
        filterProvider.addFilter(FILTER_NAME_CODEREGISTRY, SimpleBeanPropertyFilter.filterOutAllExcept(FIELD_NAME_URI));
        filterProvider.addFilter(FILTER_NAME_CODESCHEME, SimpleBeanPropertyFilter.filterOutAllExcept(FIELD_NAME_URI));
        filterProvider.addFilter(FILTER_NAME_CODE, SimpleBeanPropertyFilter.filterOutAllExcept(FIELD_NAME_URI));
        filterProvider.addFilter(FILTER_NAME_EXTERNALREFERENCE, SimpleBeanPropertyFilter.filterOutAllExcept(FIELD_NAME_URI));
        filterProvider.addFilter(FILTER_NAME_PROPERTYTYPE, SimpleBeanPropertyFilter.filterOutAllExcept(FIELD_NAME_URI));
        filterProvider.addFilter(FILTER_NAME_DATACLASSIFICATION, SimpleBeanPropertyFilter.filterOutAllExcept(FIELD_NAME_URI));
        filterProvider.addFilter(FILTER_NAME_ORGANIZATION, SimpleBeanPropertyFilter.filterOutAllExcept(FIELD_NAME_ID));
        filterProvider.setFailOnUnknownId(false);
        for (final String baseFilter : baseFilters) {
            filterProvider.removeFilter(baseFilter);
        }
        if (expand != null && !expand.isEmpty()) {
            final List<String> filterOptions = Arrays.asList(expand.split(","));
            for (final String filter : filterOptions) {
                filterProvider.removeFilter(filter);
            }
        }
        return filterProvider;
    }

    Response createErrorResponse(final int errorCode,
                                 final String errorMessage) {
        final ErrorWrapper error = new ErrorWrapper();
        final Meta meta = new Meta();
        meta.setCode(errorCode);
        meta.setMessage(errorMessage);
        error.setMeta(meta);
        return Response.status(Response.Status.NOT_FOUND).entity(error).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

    public List<String> parseStatus(final String statusCsl) {
        final Set<String> statusSet = new HashSet<>();
        if (statusCsl != null) {
            for (final String s : Arrays.asList(statusCsl.split(","))) {
                final Status status = Status.valueOf(s.toUpperCase().trim());
                statusSet.add(status.toString());
            }
        }
        return new ArrayList<String>(statusSet);
    }

    public List<String> parseDataClassifications(final String dataClassificationCsl) {
        final Set<String> dataClassificationsSet = new HashSet<>();
        if (dataClassificationCsl != null) {
            for (final String s : Arrays.asList(dataClassificationCsl.split(","))) {
                dataClassificationsSet.add(s.toUpperCase().trim());
            }
        }
        return new ArrayList<String>(dataClassificationsSet);
    }

    public void logApiRequest(final Logger logger,
                              final String method,
                              final String apiVersionPath,
                              final String apiPath) {
        logger.info(method + " " + apiVersionPath + apiPath + " requested!");
    }

    public void appendNotNull(final StringBuilder builder,
                              final String string) {
        if (string != null) {
            builder.append(string);
        }
    }

    public String createDownloadFilename(final String format,
                                         final String filename) {
        if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format)) {
            return filename + "." + FORMAT_EXCEL_XLS;
        } else if (FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            return filename + "." + FORMAT_EXCEL_XLSX;
        } else {
            return filename + "." + FORMAT_CSV;
        }
    }

    public Response streamCsvCodesOutput(final String csv) {
        return streamCsvOutput(csv, DOWNLOAD_FILENAME_CODES);
    }

    public Response streamCsvCodeSchemeDTOsOutput(final String csv) {
        return streamCsvOutput(csv, DOWNLOAD_FILENAME_CODESCHEMES);
    }

    public Response streamCsvCodeRegistriesOutput(final String csv) {
        return streamCsvOutput(csv, DOWNLOAD_FILENAME_CODEREGISTRIES);
    }

    public Response streamCsvExternalReferenceDTOsOutput(final String csv) {
        return streamCsvOutput(csv, DOWNLOAD_FILENAME_EXTERNALREFERENCES);
    }

    public Response streamCsvPropertyTypeDTOsOutput(final String csv) {
        return streamCsvOutput(csv, DOWNLOAD_FILENAME_PROPERTYTYPES);
    }

    private Response streamCsvOutput(final String csv,
                                     final String filename) {
        final StreamingOutput stream = output -> {
            try {
                output.write(csv.getBytes(StandardCharsets.UTF_8));
            } catch (final Exception e) {
                throw new WebApplicationException(e);
            }
        };
        return Response.ok(stream).header(HEADER_CONTENT_DISPOSITION, "attachment; filename = " + createDownloadFilename(FORMAT_CSV, filename)).build();
    }

    public Response streamExcelCodesOutput(final Workbook workbook) {
        return streamExcelOutput(workbook, DOWNLOAD_FILENAME_CODES);
    }

    public Response streamExcelCodeSchemeDTOsOutput(final Workbook workbook) {
        return streamExcelOutput(workbook, DOWNLOAD_FILENAME_CODESCHEMES);
    }

    public Response streamExcelCodeRegistriesOutput(final Workbook workbook) {
        return streamExcelOutput(workbook, DOWNLOAD_FILENAME_CODEREGISTRIES);
    }

    public Response streamExcelExternalReferenceDTOsOutput(final Workbook workbook) {
        return streamExcelOutput(workbook, DOWNLOAD_FILENAME_EXTERNALREFERENCES);
    }

    public Response streamExcelPropertyTypeDTOsOutput(final Workbook workbook) {
        return streamExcelOutput(workbook, DOWNLOAD_FILENAME_PROPERTYTYPES);
    }

    private Response streamExcelOutput(final Workbook workbook,
                                      final String filename) {
        final StreamingOutput stream = output -> {
            try {
                workbook.write(output);
            } catch (final Exception e) {
                throw new WebApplicationException(e);
            }
        };
        return Response.ok(stream).header(HEADER_CONTENT_DISPOSITION, "attachment; filename = " + createDownloadFilename(FORMAT_EXCEL, filename)).build();
    }

    static class FilterModifier extends ObjectWriterModifier {

        private final FilterProvider provider;

        protected FilterModifier(final FilterProvider provider) {
            this.provider = provider;
        }

        @Override
        public ObjectWriter modify(final EndpointConfigBase<?> endpoint,
                                   final MultivaluedMap<String, Object> responseHeaders,
                                   final Object valueToWrite,
                                   final ObjectWriter w,
                                   final JsonGenerator g) throws IOException {
            return w.with(provider);
        }
    }
}
