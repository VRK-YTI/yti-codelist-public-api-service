package fi.vm.yti.codelist.api.export;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.api.domain.Domain;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ExtensionSchemeDTO;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
public class ExtensionSchemeExporter extends BaseExporter {

    private final Domain domain;
    private final ExtensionExporter extensionExporter;

    public ExtensionSchemeExporter(final Domain domain,
                                   final ExtensionExporter extensionExporter) {
        this.domain = domain;
        this.extensionExporter = extensionExporter;
    }

    public String createCsv(final Set<ExtensionSchemeDTO> extensionSchemes) {
        final Set<String> prefLabelLanguages = resolveExtensionSchemePrefLabelLanguages(extensionSchemes);
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, csvSeparator, CONTENT_HEADER_CODEVALUE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ID);
        appendValue(csv, csvSeparator, CONTENT_HEADER_STATUS);
        appendValue(csv, csvSeparator, CONTENT_HEADER_PROPERTYTYPE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_CODESCHEMES);
        prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        appendValue(csv, csvSeparator, CONTENT_HEADER_STARTDATE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ENDDATE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_CREATED);
        appendValue(csv, csvSeparator, CONTENT_HEADER_MODIFIED);
        for (final ExtensionSchemeDTO extensionScheme : extensionSchemes) {
            appendValue(csv, csvSeparator, extensionScheme.getCodeValue());
            appendValue(csv, csvSeparator, extensionScheme.getId().toString());
            appendValue(csv, csvSeparator, extensionScheme.getStatus());
            appendValue(csv, csvSeparator, extensionScheme.getPropertyType().getLocalName());
            appendValue(csv, csvSeparator, getExtensionSchemeUris(extensionScheme.getCodeSchemes()));
            prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, extensionScheme.getPrefLabel().get(language)));
            appendValue(csv, csvSeparator, extensionScheme.getStartDate() != null ? formatDateWithISO8601(extensionScheme.getStartDate()) : "");
            appendValue(csv, csvSeparator, extensionScheme.getEndDate() != null ? formatDateWithISO8601(extensionScheme.getEndDate()) : "");
            appendValue(csv, csvSeparator, extensionScheme.getCreated() != null ? formatDateWithSeconds(extensionScheme.getCreated()) : "");
            appendValue(csv, csvSeparator, extensionScheme.getModified() != null ? formatDateWithSeconds(extensionScheme.getModified()) : "");
        }
        return csv.toString();
    }

    public Workbook createExcel(final Set<ExtensionSchemeDTO> extensionSchemes,
                                final String format) {
        final Workbook workbook = createWorkBook(format);
        addExtensionSchemesSheet(workbook, EXCEL_SHEET_EXTENSIONSCHEMES, extensionSchemes);
        return workbook;
    }

    public Workbook createExcel(final ExtensionSchemeDTO extensionScheme,
                                final String format) {
        final Workbook workbook = createWorkBook(format);
        final Set<ExtensionSchemeDTO> extensionSchemes = new HashSet<>();
        extensionSchemes.add(extensionScheme);
        addExtensionSchemesSheet(workbook, EXCEL_SHEET_EXTENSIONSCHEMES, extensionSchemes);
        final String extensionSheetName = truncateSheetNameWithIndex(EXCEL_SHEET_EXTENSIONS + "_" + extensionScheme.getParentCodeScheme().getCodeValue() + "_" + extensionScheme.getCodeValue(), 1);
        extensionExporter.addExtensionsSheet(workbook, extensionSheetName, domain.getExtensions(null, null, extensionScheme, null, null));
        return workbook;
    }

    public void addExtensionSchemesSheet(final Workbook workbook,
                                         final String sheetName,
                                         final Set<ExtensionSchemeDTO> extensionSchemes) {
        final Set<String> prefLabelLanguages = resolveExtensionSchemePrefLabelLanguages(extensionSchemes);
        final Sheet sheet = workbook.createSheet(sheetName);
        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ID);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODEVALUE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_STATUS);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_PROPERTYTYPE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODESCHEMES);
        for (final String language : prefLabelLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        }
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_STARTDATE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ENDDATE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CREATED);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_MODIFIED);
        rowhead.createCell(j).setCellValue(CONTENT_HEADER_EXTENSIONSSHEET);
        int i = 0;
        for (final ExtensionSchemeDTO extensionScheme : extensionSchemes) {
            final Row row = sheet.createRow(++i);
            int k = 0;
            row.createCell(k++).setCellValue(checkEmptyValue(extensionScheme.getId().toString()));
            row.createCell(k++).setCellValue(checkEmptyValue(extensionScheme.getCodeValue()));
            row.createCell(k++).setCellValue(checkEmptyValue(extensionScheme.getStatus()));
            row.createCell(k++).setCellValue(checkEmptyValue(extensionScheme.getPropertyType().getLocalName()));
            row.createCell(k++).setCellValue(checkEmptyValue(getExtensionSchemeUris(extensionScheme.getCodeSchemes())));
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(extensionScheme.getPrefLabel().get(language));
            }
            row.createCell(k++).setCellValue(extensionScheme.getStartDate() != null ? formatDateWithISO8601(extensionScheme.getStartDate()) : "");
            row.createCell(k++).setCellValue(extensionScheme.getEndDate() != null ? formatDateWithISO8601(extensionScheme.getEndDate()) : "");
            row.createCell(k++).setCellValue(extensionScheme.getCreated() != null ? formatDateWithSeconds(extensionScheme.getCreated()) : "");
            row.createCell(k++).setCellValue(extensionScheme.getModified() != null ? formatDateWithSeconds(extensionScheme.getModified()) : "");
            row.createCell(k).setCellValue(checkEmptyValue(truncateSheetNameWithIndex(EXCEL_SHEET_EXTENSIONS + "_" + extensionScheme.getParentCodeScheme().getCodeValue() + "_" + extensionScheme.getCodeValue(), i)));
        }
    }

    private String getExtensionSchemeUris(final Set<CodeSchemeDTO> codeSchemes) {
        final StringBuilder codeSchemeUris = new StringBuilder();
        int i = 0;
        if (codeSchemes != null) {
            for (final CodeSchemeDTO codeScheme : codeSchemes) {
                i++;
                codeSchemeUris.append(codeScheme.getUri().trim());
                if (i < codeSchemes.size()) {
                    codeSchemeUris.append(";");
                }
                i++;
            }
        }
        return codeSchemeUris.toString();
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
