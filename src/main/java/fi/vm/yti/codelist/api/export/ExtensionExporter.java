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
import fi.vm.yti.codelist.common.dto.ExtensionDTO;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
public class ExtensionExporter extends BaseExporter {

    private static final String LOCALNAME_CROSS_REFERENCE_LIST = "crossReferenceList";

    private final Domain domain;
    private final MemberExporter memberExporter;

    public ExtensionExporter(final Domain domain,
                             final MemberExporter memberExporter) {
        this.domain = domain;
        this.memberExporter = memberExporter;
    }

    public String createCsv(final Set<ExtensionDTO> extensions) {
        final Set<String> prefLabelLanguages = resolveExtensionPrefLabelLanguages(extensions);
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, CONTENT_HEADER_CODEVALUE);
        appendValue(csv, CONTENT_HEADER_URI);
        appendValue(csv, CONTENT_HEADER_STATUS);
        appendValue(csv, CONTENT_HEADER_PROPERTYTYPE);
        appendValue(csv, CONTENT_HEADER_CODESCHEMES);
        prefLabelLanguages.forEach(language -> appendValue(csv, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        appendValue(csv, CONTENT_HEADER_STARTDATE);
        appendValue(csv, CONTENT_HEADER_ENDDATE);
        appendValue(csv, CONTENT_HEADER_CREATED);
        appendValue(csv, CONTENT_HEADER_MODIFIED);
        for (final ExtensionDTO extension : extensions) {
            appendValue(csv, extension.getCodeValue());
            appendValue(csv, extension.getUri());
            appendValue(csv, extension.getStatus());
            appendValue(csv, extension.getPropertyType().getLocalName());
            appendValue(csv, getCodeSchemeUris(extension.getCodeSchemes()));
            prefLabelLanguages.forEach(language -> appendValue(csv, getExtensionPrefLabel(extension, language)));
            appendValue(csv, extension.getStartDate() != null ? formatDateWithISO8601(extension.getStartDate()) : "");
            appendValue(csv, extension.getEndDate() != null ? formatDateWithISO8601(extension.getEndDate()) : "");
            appendValue(csv, extension.getCreated() != null ? formatDateWithSeconds(extension.getCreated()) : "");
            appendValue(csv, extension.getModified() != null ? formatDateWithSeconds(extension.getModified()) : "");
        }
        return csv.toString();
    }

    public Workbook createExcel(final Set<ExtensionDTO> extensions,
                                final String format) {
        final Workbook workbook = createWorkBook(format);
        addExtensionSheet(workbook, EXCEL_SHEET_EXTENSIONS, extensions);
        return workbook;
    }

    public Workbook createExcel(final ExtensionDTO extension,
                                final String format,
                                final boolean exportAsSimplifiedCrossReferenceList) {
        final Workbook workbook = createWorkBook(format);
        final Set<ExtensionDTO> extensions = new HashSet<>();
        extensions.add(extension);
        if (!exportAsSimplifiedCrossReferenceList) {
            addExtensionSheet(workbook, EXCEL_SHEET_EXTENSIONS, extensions);
        }
        final String extensionSheetName = truncateSheetNameWithIndex(EXCEL_SHEET_MEMBERS + "_" + extension.getParentCodeScheme().getCodeValue() + "_" + extension.getCodeValue(), 1);
        if (exportAsSimplifiedCrossReferenceList) {
            memberExporter.addMembersSheetWithCrossRerefences(extension, workbook, domain.getMembers(extension, null));
        } else {
            memberExporter.addMembersSheet(extension, workbook, extensionSheetName, domain.getMembers(extension, null));
            if (LOCALNAME_CROSS_REFERENCE_LIST.equalsIgnoreCase(extension.getPropertyType().getLocalName())) { //Cross-Reference List containing sheet will always be included as well in the normal Excel
                memberExporter.addMembersSheetWithCrossRerefences(extension, workbook, domain.getMembers(extension, null));
            }
        }
        return workbook;
    }

    public void addExtensionSheet(final Workbook workbook,
                                  final String sheetName,
                                  final Set<ExtensionDTO> extensions) {
        final Set<String> prefLabelLanguages = resolveExtensionPrefLabelLanguages(extensions);
        final Sheet sheet = workbook.createSheet(sheetName);
        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODEVALUE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_URI);
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
        rowhead.createCell(j).setCellValue(CONTENT_HEADER_MEMBERSSHEET);
        int i = 0;
        for (final ExtensionDTO extension : extensions) {
            final Row row = sheet.createRow(++i);
            int k = 0;
            row.createCell(k++).setCellValue(checkEmptyValue(extension.getCodeValue()));
            row.createCell(k++).setCellValue(extension.getUri());
            row.createCell(k++).setCellValue(checkEmptyValue(extension.getStatus()));
            row.createCell(k++).setCellValue(checkEmptyValue(extension.getPropertyType().getLocalName()));
            row.createCell(k++).setCellValue(checkEmptyValue(getCodeSchemeUris(extension.getCodeSchemes())));
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(getExtensionPrefLabel(extension, language));
            }
            row.createCell(k++).setCellValue(extension.getStartDate() != null ? formatDateWithISO8601(extension.getStartDate()) : "");
            row.createCell(k++).setCellValue(extension.getEndDate() != null ? formatDateWithISO8601(extension.getEndDate()) : "");
            row.createCell(k++).setCellValue(extension.getCreated() != null ? formatDateWithSeconds(extension.getCreated()) : "");
            row.createCell(k++).setCellValue(extension.getModified() != null ? formatDateWithSeconds(extension.getModified()) : "");
            row.createCell(k).setCellValue(checkEmptyValue(truncateSheetNameWithIndex(EXCEL_SHEET_MEMBERS + "_" + extension.getParentCodeScheme().getCodeValue() + "_" + extension.getCodeValue(), i)));
        }
    }

    private String getCodeSchemeUris(final Set<CodeSchemeDTO> codeSchemes) {
        final StringBuilder codeSchemeUris = new StringBuilder();
        int i = 0;
        if (codeSchemes != null) {
            for (final CodeSchemeDTO codeScheme : codeSchemes) {
                i++;
                codeSchemeUris.append(codeScheme.getUri().trim());
                if (i < codeSchemes.size()) {
                    codeSchemeUris.append(";");
                }
            }
        }
        return codeSchemeUris.toString();
    }

    private Set<String> resolveExtensionPrefLabelLanguages(final Set<ExtensionDTO> extensions) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final ExtensionDTO extension : extensions) {
            final Map<String, String> prefLabel = extension.getPrefLabel();
            if (prefLabel != null && !prefLabel.isEmpty()) {
                languages.addAll(prefLabel.keySet());
            }
        }
        return languages;
    }
}
