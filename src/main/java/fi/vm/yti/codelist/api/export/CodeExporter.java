package fi.vm.yti.codelist.api.export;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.common.dto.CodeDTO;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
public class CodeExporter extends BaseExporter {

    public String createCsv(final Set<CodeDTO> codes) {
        int flatInt = 1;
        final Set<String> prefLabelLanguages = resolveCodePrefLabelLanguages(codes);
        final Set<String> definitionLanguages = resolveCodeDefinitionLanguages(codes);
        final Set<String> descriptionLanguages = resolveCodeDescriptionLanguages(codes);
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, CONTENT_HEADER_CODEVALUE);
        appendValue(csv, CONTENT_HEADER_URI);
        appendValue(csv, CONTENT_HEADER_ORDER);
        appendValue(csv, CONTENT_HEADER_BROADER);
        appendValue(csv, CONTENT_HEADER_STATUS);
        prefLabelLanguages.forEach(language -> appendValue(csv, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        definitionLanguages.forEach(language -> appendValue(csv, CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase()));
        descriptionLanguages.forEach(language -> appendValue(csv, CONTENT_HEADER_DESCRIPTION_PREFIX + language.toUpperCase()));
        appendValue(csv, CONTENT_HEADER_SHORTNAME);
        appendValue(csv, CONTENT_HEADER_CONCEPTURI);
        appendValue(csv, CONTENT_HEADER_SUBCODESCHEME);
        appendValue(csv, CONTENT_HEADER_HIERARCHYLEVEL);
        appendValue(csv, CONTENT_HEADER_STARTDATE);
        appendValue(csv, CONTENT_HEADER_ENDDATE);
        appendValue(csv, CONTENT_HEADER_CREATED);
        appendValue(csv, CONTENT_HEADER_MODIFIED);
        appendValue(csv, CONTENT_HEADER_HREF, true);
        for (final CodeDTO code : codes) {
            appendValue(csv, code.getCodeValue());
            appendValue(csv, code.getUri());
            appendValue(csv, code.getOrder() != null ? code.getOrder().toString() : String.valueOf(flatInt));
            appendValue(csv, code.getBroaderCode() != null ? code.getBroaderCode().getCodeValue() : "");
            appendValue(csv, code.getStatus());
            prefLabelLanguages.forEach(language -> appendValue(csv, getCodePrefLabel(code, language)));
            definitionLanguages.forEach(language -> appendValue(csv, getCodeDefinition(code, language)));
            descriptionLanguages.forEach(language -> appendValue(csv, getCodeDescription(code, language)));
            appendValue(csv, code.getShortName());
            appendValue(csv, code.getConceptUriInVocabularies());
            appendValue(csv, code.getSubCodeScheme() != null ? code.getSubCodeScheme().getUri() : null);
            appendValue(csv, code.getHierarchyLevel() != null ? code.getHierarchyLevel().toString() : null);
            appendValue(csv, code.getStartDate() != null ? formatDateWithISO8601(code.getStartDate()) : "");
            appendValue(csv, code.getEndDate() != null ? formatDateWithISO8601(code.getEndDate()) : "");
            appendValue(csv, code.getCreated() != null ? formatDateWithSeconds(code.getCreated()) : "");
            appendValue(csv, code.getModified() != null ? formatDateWithSeconds(code.getModified()) : "");
            appendValue(csv, formatExternalReferencesToString(code.getExternalReferences()), true);
            flatInt++;
        }
        return csv.toString();
    }

    public Workbook createExcel(final Set<CodeDTO> codes,
                                final String format) {
        final Workbook workbook = createWorkBook(format);
        addCodeSheet(workbook, EXCEL_SHEET_CODES, codes);
        return workbook;
    }

    public void addCodeSheet(final Workbook workbook,
                             final String sheetName,
                             final Set<CodeDTO> codes) {
        final Set<String> prefLabelLanguages = resolveCodePrefLabelLanguages(codes);
        final Set<String> definitionLanguages = resolveCodeDefinitionLanguages(codes);
        final Set<String> descriptionLanguages = resolveCodeDescriptionLanguages(codes);
        final Sheet sheet = workbook.createSheet(sheetName);
        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODEVALUE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_URI);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_BROADER);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_STATUS);
        for (final String language : prefLabelLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        }
        for (final String language : definitionLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase());
        }
        for (final String language : descriptionLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_DESCRIPTION_PREFIX + language.toUpperCase());
        }
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_SHORTNAME);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CONCEPTURI);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_SUBCODESCHEME);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_HIERARCHYLEVEL);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ORDER);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_STARTDATE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ENDDATE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CREATED);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_MODIFIED);
        rowhead.createCell(j).setCellValue(CONTENT_HEADER_HREF);
        int i = 1;
        int flatInt = 1;
        for (final CodeDTO code : codes) {
            final Row row = sheet.createRow(i++);
            int k = 0;
            row.createCell(k++).setCellValue(code.getCodeValue());
            row.createCell(k++).setCellValue(code.getUri());
            row.createCell(k++).setCellValue(code.getBroaderCode() != null ? code.getBroaderCode().getCodeValue() : "");
            row.createCell(k++).setCellValue(code.getStatus());
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(getCodePrefLabel(code, language));
            }
            for (final String language : definitionLanguages) {
                row.createCell(k++).setCellValue(getCodeDefinition(code, language));
            }
            for (final String language : descriptionLanguages) {
                row.createCell(k++).setCellValue(getCodeDescription(code, language));
            }
            row.createCell(k++).setCellValue(checkEmptyValue(code.getShortName()));
            row.createCell(k++).setCellValue(checkEmptyValue(code.getConceptUriInVocabularies()));
            row.createCell(k++).setCellValue(checkEmptyValue(code.getSubCodeScheme() != null ? code.getSubCodeScheme().getUri() : null));
            row.createCell(k++).setCellValue(checkEmptyValue(code.getHierarchyLevel() != null ? code.getHierarchyLevel().toString() : null));
            row.createCell(k++).setCellValue(checkEmptyValue(code.getOrder() != null ? code.getOrder().toString() : Integer.toString(flatInt)));
            row.createCell(k++).setCellValue(code.getStartDate() != null ? formatDateWithISO8601(code.getStartDate()) : "");
            row.createCell(k++).setCellValue(code.getEndDate() != null ? formatDateWithISO8601(code.getEndDate()) : "");
            row.createCell(k++).setCellValue(code.getCreated() != null ? formatDateWithSeconds(code.getCreated()) : "");
            row.createCell(k++).setCellValue(code.getModified() != null ? formatDateWithSeconds(code.getModified()) : "");
            row.createCell(k).setCellValue(checkEmptyValue(formatExternalReferencesToString(code.getExternalReferences())));
            flatInt++;
        }
    }

    private Set<String> resolveCodeDefinitionLanguages(final Set<CodeDTO> codes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeDTO code : codes) {
            final Map<String, String> definition = code.getDefinition();
            if (definition != null && !definition.isEmpty()) {
                languages.addAll(definition.keySet());
            }
        }
        return languages;
    }

    private Set<String> resolveCodeDescriptionLanguages(final Set<CodeDTO> codes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeDTO code : codes) {
            final Map<String, String> description = code.getDescription();
            if (description != null && !description.isEmpty()) {
                languages.addAll(description.keySet());
            }
        }
        return languages;
    }
}
