package fi.vm.yti.codelist.api.export;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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

        final Map<UUID, String> codeValueIdMap = new HashMap<>();
        for (final CodeDTO code : codes) {
            codeValueIdMap.put(code.getId(), code.getCodeValue());
        }
        final Set<String> prefLabelLanguages = resolveCodePrefLabelLanguages(codes);
        final Set<String> definitionLanguages = resolveCodeDefinitionLanguages(codes);
        final Set<String> descriptionLanguages = resolveCodeDescriptionLanguages(codes);
        final DateFormat dateFormat = new SimpleDateFormat(DATEFORMAT);
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();

        appendValue(csv, csvSeparator, CONTENT_HEADER_ORDER);
        appendValue(csv, csvSeparator, CONTENT_HEADER_CODEVALUE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_BROADER);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ID);
        appendValue(csv, csvSeparator, CONTENT_HEADER_STATUS);
        prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        definitionLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase()));
        descriptionLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_DESCRIPTION_PREFIX + language.toUpperCase()));
        appendValue(csv, csvSeparator, CONTENT_HEADER_SHORTNAME);
        appendValue(csv, csvSeparator, CONTENT_HEADER_HIERARCHYLEVEL);
        appendValue(csv, csvSeparator, CONTENT_HEADER_STARTDATE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ENDDATE, true);
        for (final CodeDTO code : codes) {
            appendValue(csv, csvSeparator, code.getOrder() != null ? code.getOrder().toString() : flatInt.toString());
            appendValue(csv, csvSeparator, code.getCodeValue());
            appendValue(csv, csvSeparator, codeValueIdMap.get(code.getBroaderCodeId()));
            appendValue(csv, csvSeparator, code.getId().toString());
            appendValue(csv, csvSeparator, code.getStatus());
            prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, code.getPrefLabel().get(language)));
            definitionLanguages.forEach(language -> appendValue(csv, csvSeparator, code.getDefinition().get(language)));
            descriptionLanguages.forEach(language -> appendValue(csv, csvSeparator, code.getDescription().get(language)));
            appendValue(csv, csvSeparator, code.getShortName());
            appendValue(csv, csvSeparator, code.getHierarchyLevel() != null ? code.getHierarchyLevel().toString() : null);
            appendValue(csv, csvSeparator, code.getStartDate() != null ? dateFormat.format(code.getStartDate()) : "");
            appendValue(csv, csvSeparator, code.getEndDate() != null ? dateFormat.format(code.getEndDate()) : "", true);

            flatInt++;
        }
        return csv.toString();
    }

    public Workbook createExcel(final Set<CodeDTO> codes,
                                final String format) {
        final Map<UUID, String> codeValueIdMap = new HashMap<>();
        for (final CodeDTO code : codes) {
            codeValueIdMap.put(code.getId(), code.getCodeValue());
        }
        final Workbook workbook = createWorkBook(format);
        final Set<String> prefLabelLanguages = resolveCodePrefLabelLanguages(codes);
        final Set<String> definitionLanguages = resolveCodeDefinitionLanguages(codes);
        final Set<String> descriptionLanguages = resolveCodeDescriptionLanguages(codes);
        final DateFormat dateFormat = new SimpleDateFormat(DATEFORMAT);
        final Sheet sheet = workbook.createSheet(EXCEL_SHEET_CODES);
        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODEVALUE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_BROADER);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ID);
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
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_HIERARCHYLEVEL);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ORDER);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_STARTDATE);
        rowhead.createCell(j).setCellValue(CONTENT_HEADER_ENDDATE);
        int i = 1;
        Integer flatInt = 1;

        for (final CodeDTO code : codes) {
            final Row row = sheet.createRow(i++);
            int k = 0;

            row.createCell(k++).setCellValue(code.getCodeValue());
            row.createCell(k++).setCellValue(checkEmptyValue(codeValueIdMap.get(code.getBroaderCodeId())));
            row.createCell(k++).setCellValue(code.getId().toString());
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
            row.createCell(k++).setCellValue(checkEmptyValue(code.getHierarchyLevel() != null ? code.getHierarchyLevel().toString() : null));
            row.createCell(k++).setCellValue(checkEmptyValue(code.getOrder() != null ? code.getOrder().toString() : flatInt.toString()));
            row.createCell(k++).setCellValue(code.getStartDate() != null ? dateFormat.format(code.getStartDate()) : "");
            row.createCell(k).setCellValue(code.getEndDate() != null ? dateFormat.format(code.getEndDate()) : "");

            flatInt++;
        }
        return workbook;
    }

    private Set<String> resolveCodePrefLabelLanguages(final Set<CodeDTO> codes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeDTO code : codes) {
            final Map<String, String> prefLabel = code.getPrefLabel();
            languages.addAll(prefLabel.keySet());
        }
        return languages;
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
