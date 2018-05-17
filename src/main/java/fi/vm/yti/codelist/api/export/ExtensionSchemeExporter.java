package fi.vm.yti.codelist.api.export;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.api.exception.YtiCodeListException;
import fi.vm.yti.codelist.common.dto.ErrorModel;
import fi.vm.yti.codelist.common.dto.ExtensionSchemeDTO;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
public class ExtensionSchemeExporter extends BaseExporter {

    private static final Logger LOG = LoggerFactory.getLogger(ExtensionSchemeExporter.class);

    public String createCsv(final Set<ExtensionSchemeDTO> extensionSchemes) {
        final Set<String> prefLabelLanguages = resolveExtensionSchemePrefLabelLanguages(extensionSchemes);
        final DateFormat dateFormat = new SimpleDateFormat(DATEFORMAT);
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, csvSeparator, CONTENT_HEADER_ID);
        appendValue(csv, csvSeparator, CONTENT_HEADER_CODEVALUE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_STATUS);
        appendValue(csv, csvSeparator, CONTENT_HEADER_PROPERTYTYPE);
        prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        appendValue(csv, csvSeparator, CONTENT_HEADER_STARTDATE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ENDDATE, true);
        for (final ExtensionSchemeDTO extensionScheme : extensionSchemes) {
            appendValue(csv, csvSeparator, extensionScheme.getId().toString());
            appendValue(csv, csvSeparator, extensionScheme.getCodeValue());
            appendValue(csv, csvSeparator, extensionScheme.getStatus());
            appendValue(csv, csvSeparator, extensionScheme.getPropertyType().getLocalName());
            prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, extensionScheme.getPrefLabel().get(language)));
            appendValue(csv, csvSeparator, extensionScheme.getStartDate() != null ? dateFormat.format(extensionScheme.getStartDate()) : "");
            appendValue(csv, csvSeparator, extensionScheme.getEndDate() != null ? dateFormat.format(extensionScheme.getEndDate()) : "", true);
        }
        return csv.toString();
    }

    public Workbook createExcel(final Set<ExtensionSchemeDTO> extensionSchemes,
                                final String format) {
        try (final Workbook workbook = createWorkBook(format)) {
            addExtensionSchemesSheet(workbook, EXCEL_SHEET_EXTENSIONSCHEMES, extensionSchemes);
            return workbook;
        } catch (final IOException e) {
            LOG.error("Error creating Excel during export!", e);
            throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "Excel output generation failed!"));
        }
    }

    public void addExtensionSchemesSheet(final Workbook workbook,
                                         final String sheetName,
                                         final Set<ExtensionSchemeDTO> extensionSchemes) {
        final Set<String> prefLabelLanguages = resolveExtensionSchemePrefLabelLanguages(extensionSchemes);
        final DateFormat dateFormat = new SimpleDateFormat(DATEFORMAT);
        final Sheet sheet = workbook.createSheet(sheetName);
        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ID);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODEVALUE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_STATUS);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_PROPERTYTYPE);
        for (final String language : prefLabelLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        }
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_STARTDATE);
        rowhead.createCell(j).setCellValue(CONTENT_HEADER_ENDDATE);
        int i = 1;
        for (final ExtensionSchemeDTO extensionScheme : extensionSchemes) {
            final Row row = sheet.createRow(i++);
            int k = 0;
            row.createCell(k++).setCellValue(checkEmptyValue(extensionScheme.getId().toString()));
            row.createCell(k++).setCellValue(checkEmptyValue(extensionScheme.getCodeValue()));
            row.createCell(k++).setCellValue(checkEmptyValue(extensionScheme.getStatus()));
            row.createCell(k++).setCellValue(checkEmptyValue(extensionScheme.getPropertyType().getLocalName()));
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(extensionScheme.getPrefLabel().get(language));
            }
            row.createCell(k++).setCellValue(extensionScheme.getStartDate() != null ? dateFormat.format(extensionScheme.getStartDate()) : "");
            row.createCell(k).setCellValue(extensionScheme.getEndDate() != null ? dateFormat.format(extensionScheme.getEndDate()) : "");
        }
    }

    private Set<String> resolveExtensionSchemePrefLabelLanguages(final Set<ExtensionSchemeDTO> extensionSchemes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final ExtensionSchemeDTO codeScheme : extensionSchemes) {
            final Map<String, String> prefLabel = codeScheme.getPrefLabel();
            languages.addAll(prefLabel.keySet());
        }
        return languages;
    }
}
