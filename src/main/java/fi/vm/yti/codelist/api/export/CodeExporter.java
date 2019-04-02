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
        Integer flatInt = 1;
        final Set<String> prefLabelLanguages = resolveCodePrefLabelLanguages(codes);
        final Set<String> definitionLanguages = resolveCodeDefinitionLanguages(codes);
        final Set<String> descriptionLanguages = resolveCodeDescriptionLanguages(codes);
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, csvSeparator, CONTENT_HEADER_CODEVALUE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ORDER);
        appendValue(csv, csvSeparator, CONTENT_HEADER_BROADER);
        appendValue(csv, csvSeparator, CONTENT_HEADER_STATUS);
        prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        definitionLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase()));
        descriptionLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_DESCRIPTION_PREFIX + language.toUpperCase()));
        appendValue(csv, csvSeparator, CONTENT_HEADER_SHORTNAME);
        appendValue(csv, csvSeparator, CONTENT_HEADER_CONCEPTURI);
        appendValue(csv, csvSeparator, CONTENT_HEADER_SUBCODESCHEME);
        appendValue(csv, csvSeparator, CONTENT_HEADER_HIERARCHYLEVEL);
        appendValue(csv, csvSeparator, CONTENT_HEADER_STARTDATE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ENDDATE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_CREATED);
        appendValue(csv, csvSeparator, CONTENT_HEADER_MODIFIED);
        appendValue(csv, csvSeparator, CONTENT_HEADER_HREF, true);
        for (final CodeDTO code : codes) {
            appendValue(csv, csvSeparator, code.getCodeValue());
            appendValue(csv, csvSeparator, code.getOrder() != null ? code.getOrder().toString() : flatInt.toString());
            appendValue(csv, csvSeparator, code.getBroaderCode() != null ? code.getBroaderCode().getCodeValue() : "");
            appendValue(csv, csvSeparator, code.getStatus());
            prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, code.getPrefLabel().get(language)));
            definitionLanguages.forEach(language -> appendValue(csv, csvSeparator, code.getDefinition().get(language)));
            descriptionLanguages.forEach(language -> appendValue(csv, csvSeparator, code.getDescription().get(language)));
            appendValue(csv, csvSeparator, code.getShortName());
            appendValue(csv, csvSeparator, code.getConceptUriInVocabularies());
            appendValue(csv, csvSeparator, code.getSubCodeScheme() != null ? code.getSubCodeScheme().getUri() : null);
            appendValue(csv, csvSeparator, code.getHierarchyLevel() != null ? code.getHierarchyLevel().toString() : null);
            appendValue(csv, csvSeparator, code.getStartDate() != null ? formatDateWithISO8601(code.getStartDate()) : "");
            appendValue(csv, csvSeparator, code.getEndDate() != null ? formatDateWithISO8601(code.getEndDate()) : "");
            appendValue(csv, csvSeparator, code.getCreated() != null ? formatDateWithSeconds(code.getCreated()) : "");
            appendValue(csv, csvSeparator, code.getModified() != null ? formatDateWithSeconds(code.getModified()) : "");
            appendValue(csv, csvSeparator, formatExternalReferencesToString(code.getExternalReferences()), true);
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
            row.createCell(k++).setCellValue(code.getBroaderCode() != null ? code.getBroaderCode().getCodeValue() : "");
            row.createCell(k++).setCellValue(code.getStatus());
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(code.getPrefLabel().get(language));
            }
            for (final String language : definitionLanguages) {
                row.createCell(k++).setCellValue(code.getDefinition().get(language));
            }
            for (final String language : descriptionLanguages) {
                row.createCell(k++).setCellValue(code.getDescription().get(language));
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
            languages.addAll(definition.keySet());
        }
        return languages;
    }

    private Set<String> resolveCodeDescriptionLanguages(final Set<CodeDTO> codes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeDTO code : codes) {
            final Map<String, String> description = code.getDescription();
            languages.addAll(description.keySet());
        }
        return languages;
    }
}
