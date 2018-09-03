package fi.vm.yti.codelist.api.export;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.common.dto.ExtensionDTO;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
public class ExtensionExporter extends BaseExporter {

    public String createCsv(final Set<ExtensionDTO> extensions) {
        final Set<String> prefLabelLanguages = resolveExtensionPrefLabelLanguages(extensions);
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, csvSeparator, CONTENT_HEADER_EXTENSIONVALUE);
        prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        appendValue(csv, csvSeparator, CONTENT_HEADER_ID);
        appendValue(csv, csvSeparator, CONTENT_HEADER_CODE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_RELATION);
        appendValue(csv, csvSeparator, CONTENT_HEADER_STARTDATE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ENDDATE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_CREATED);
        appendValue(csv, csvSeparator, CONTENT_HEADER_MODIFIED);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ORDER, true);
        for (final ExtensionDTO extension : extensions) {
            appendValue(csv, csvSeparator, extension.getExtensionValue());
            prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, extension.getPrefLabel().get(language)));
            appendValue(csv, csvSeparator, extension.getId().toString());
            appendValue(csv, csvSeparator, extension.getCode() != null ? extension.getCode().getCodeValue() : "");
            appendValue(csv, csvSeparator, extension.getExtension() != null ? extension.getExtension().getId().toString() : "");
            appendValue(csv, csvSeparator, extension.getStartDate() != null ? formatDateWithISO8601(extension.getStartDate()) : "");
            appendValue(csv, csvSeparator, extension.getEndDate() != null ? formatDateWithISO8601(extension.getEndDate()) : "");
            appendValue(csv, csvSeparator, extension.getCreated() != null ? formatDateWithSeconds(extension.getCreated()) : "");
            appendValue(csv, csvSeparator, extension.getModified() != null ? formatDateWithSeconds(extension.getModified()) : "");
            appendValue(csv, csvSeparator, extension.getOrder().toString(), true);
        }
        return csv.toString();
    }

    public Workbook createExcel(final Set<ExtensionDTO> extensions,
                                final String format) {
        final Workbook workbook = createWorkBook(format);
        addExtensionsSheet(workbook, EXCEL_SHEET_EXTENSIONS, extensions);
        return workbook;
    }

    public void addExtensionsSheet(final Workbook workbook,
                                   final String sheetName,
                                   final Set<ExtensionDTO> extensions) {
        final Set<String> prefLabelLanguages = resolveExtensionPrefLabelLanguages(extensions);
        final Sheet sheet = workbook.createSheet(sheetName);
        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ID);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_EXTENSIONVALUE);
        for (final String language : prefLabelLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        }
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_RELATION);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_STARTDATE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ENDDATE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CREATED);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_MODIFIED);
        rowhead.createCell(j).setCellValue(CONTENT_HEADER_ORDER);
        int i = 1;
        for (final ExtensionDTO extension : extensions) {
            final Row row = sheet.createRow(i++);
            int k = 0;
            row.createCell(k++).setCellValue(checkEmptyValue(extension.getId().toString()));
            row.createCell(k++).setCellValue(checkEmptyValue(extension.getExtensionValue()));
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(extension.getPrefLabel().get(language));
            }
            if (extension.getCode() != null) {
                row.createCell(k++).setCellValue(checkEmptyValue(extension.getCode().getUri()));
            } else {
                row.createCell(k++).setCellValue("");
            }
            if (extension.getExtension() != null && extension.getExtension().getCode() != null) {
                row.createCell(k++).setCellValue(checkEmptyValue(extension.getExtension().getId().toString()));
            } else {
                row.createCell(k++).setCellValue("");
            }
            row.createCell(k++).setCellValue(extension.getStartDate() != null ? formatDateWithISO8601(extension.getStartDate()) : "");
            row.createCell(k++).setCellValue(extension.getEndDate() != null ? formatDateWithISO8601(extension.getEndDate()) : "");
            row.createCell(k++).setCellValue(extension.getCreated() != null ? formatDateWithSeconds(extension.getCreated()) : "");
            row.createCell(k++).setCellValue(extension.getModified() != null ? formatDateWithSeconds(extension.getModified()) : "");
            row.createCell(k).setCellValue(checkEmptyValue(extension.getOrder() != null ? extension.getOrder().toString() : ""));
        }
    }

    private Set<String> resolveExtensionPrefLabelLanguages(final Set<ExtensionDTO> extensions) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final ExtensionDTO extension : extensions) {
            final Map<String, String> prefLabel = extension.getPrefLabel();
            languages.addAll(prefLabel.keySet());
        }
        return languages;
    }
}
