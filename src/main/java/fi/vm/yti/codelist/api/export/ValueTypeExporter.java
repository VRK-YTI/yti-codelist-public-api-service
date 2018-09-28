package fi.vm.yti.codelist.api.export;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.common.dto.ValueTypeDTO;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
public class ValueTypeExporter extends BaseExporter {

    public String createCsv(final Set<ValueTypeDTO> valueTypes) {
        final Set<String> prefLabelLanguages = resolveValueTypePrefLabelLanguages(valueTypes);
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, csvSeparator, CONTENT_HEADER_LOCALNAME);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ID);
        appendValue(csv, csvSeparator, CONTENT_HEADER_TYPEURI);
        appendValue(csv, csvSeparator, CONTENT_HEADER_URI);
        appendValue(csv, csvSeparator, CONTENT_HEADER_REGEXP);
        prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        appendValue(csv, csvSeparator, CONTENT_HEADER_REQUIRED, true);
        csv.append("\n");
        for (final ValueTypeDTO valueType : valueTypes) {
            appendValue(csv, csvSeparator, valueType.getLocalName());
            appendValue(csv, csvSeparator, valueType.getId().toString());
            appendValue(csv, csvSeparator, valueType.getTypeUri());
            appendValue(csv, csvSeparator, valueType.getUri());
            appendValue(csv, csvSeparator, valueType.getRegexp(), true);
            prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, valueType.getPrefLabel().get(language)));
            appendValue(csv, csvSeparator, Boolean.toString(valueType.getRequired()), true);
            csv.append("\n");
        }
        return csv.toString();
    }

    public Workbook createExcel(final Set<ValueTypeDTO> valueTypes,
                                final String format) {
        final Workbook workbook = createWorkBook(format);
        final Set<String> prefLabelLanguages = resolveValueTypePrefLabelLanguages(valueTypes);
        final Sheet sheet = workbook.createSheet(EXCEL_SHEET_VALUETYPES);
        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ID);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_LOCALNAME);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_TYPEURI);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_URI);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_REGEXP);
        for (final String language : prefLabelLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        }
        rowhead.createCell(j).setCellValue(CONTENT_HEADER_REQUIRED);
        int i = 1;
        for (final ValueTypeDTO valueType : valueTypes) {
            final Row row = sheet.createRow(i++);
            int k = 0;
            row.createCell(k++).setCellValue(checkEmptyValue(valueType.getId().toString()));
            row.createCell(k++).setCellValue(checkEmptyValue(valueType.getLocalName()));
            row.createCell(k++).setCellValue(checkEmptyValue(valueType.getTypeUri()));
            row.createCell(k++).setCellValue(checkEmptyValue(valueType.getUri()));
            row.createCell(k++).setCellValue(checkEmptyValue(valueType.getRegexp()));
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(valueType.getPrefLabel().get(language));
            }
            row.createCell(k).setCellValue(Boolean.toString(valueType.getRequired()));
        }
        return workbook;
    }

    private Set<String> resolveValueTypePrefLabelLanguages(final Set<ValueTypeDTO> valueTypes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final ValueTypeDTO valueType : valueTypes) {
            final Map<String, String> prefLabel = valueType.getPrefLabel();
            languages.addAll(prefLabel.keySet());
        }
        return languages;
    }
}
