package fi.vm.yti.codelist.api.export;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.common.dto.PropertyTypeDTO;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
public class PropertyTypeExporter extends BaseExporter {

    public String createCsv(final Set<PropertyTypeDTO> propertyTypes) {
        final DateFormat dateFormat = new SimpleDateFormat(DATEFORMAT);
        final Set<String> prefLabelLanguages = resolvePropertyTypePrefLabelLanguages(propertyTypes);
        final Set<String> definitionLanguages = resolvePropertyTypeDefinitionLanguages(propertyTypes);
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, csvSeparator, CONTENT_HEADER_ID);
        appendValue(csv, csvSeparator, CONTENT_HEADER_LOCALNAME);
        appendValue(csv, csvSeparator, CONTENT_HEADER_TYPE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_PROPERTYURI);
        appendValue(csv, csvSeparator, CONTENT_HEADER_CONTEXT);
        prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        definitionLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase()));
        appendValue(csv, csvSeparator, CONTENT_HEADER_CREATED);
        appendValue(csv, csvSeparator, CONTENT_HEADER_MODIFIED, true);
        csv.append("\n");
        for (final PropertyTypeDTO propertyType : propertyTypes) {
            appendValue(csv, csvSeparator, propertyType.getId().toString());
            appendValue(csv, csvSeparator, propertyType.getLocalName());
            appendValue(csv, csvSeparator, propertyType.getType());
            appendValue(csv, csvSeparator, propertyType.getPropertyUri());
            appendValue(csv, csvSeparator, propertyType.getContext());
            prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, propertyType.getPrefLabel().get(language)));
            definitionLanguages.forEach(language -> appendValue(csv, csvSeparator, propertyType.getDefinition().get(language)));
            appendValue(csv, csvSeparator, propertyType.getCreated() != null ? dateFormat.format(propertyType.getCreated()) : "");
            appendValue(csv, csvSeparator, propertyType.getModified() != null ? dateFormat.format(propertyType.getModified()) : "", true);
            csv.append("\n");
        }
        return csv.toString();
    }

    public Workbook createExcel(final Set<PropertyTypeDTO> propertyTypes,
                                final String format) {
        final DateFormat dateFormat = new SimpleDateFormat(DATEFORMAT);
        final Workbook workbook = createWorkBook(format);
        final Set<String> prefLabelLanguages = resolvePropertyTypePrefLabelLanguages(propertyTypes);
        final Set<String> definitionLanguages = resolvePropertyTypeDefinitionLanguages(propertyTypes);
        final Sheet sheet = workbook.createSheet(EXCEL_SHEET_PROPERTYTYPES);
        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ID);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_LOCALNAME);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_TYPE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_PROPERTYURI);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CONTEXT);
        for (final String language : prefLabelLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        }
        for (final String language : definitionLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase());
        }
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CREATED);
        rowhead.createCell(j).setCellValue(CONTENT_HEADER_MODIFIED);
        int i = 1;
        for (final PropertyTypeDTO propertyType : propertyTypes) {
            final Row row = sheet.createRow(i++);
            int k = 0;
            row.createCell(k++).setCellValue(checkEmptyValue(propertyType.getId().toString()));
            row.createCell(k++).setCellValue(checkEmptyValue(propertyType.getLocalName()));
            row.createCell(k++).setCellValue(checkEmptyValue(propertyType.getType()));
            row.createCell(k++).setCellValue(checkEmptyValue(propertyType.getPropertyUri()));
            row.createCell(k++).setCellValue(checkEmptyValue(propertyType.getContext()));
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(propertyType.getPrefLabel().get(language));
            }
            for (final String language : definitionLanguages) {
                row.createCell(k++).setCellValue(propertyType.getDefinition().get(language));
            }
            row.createCell(k++).setCellValue(propertyType.getCreated() != null ? dateFormat.format(propertyType.getCreated()) : "");
            row.createCell(k).setCellValue(propertyType.getModified() != null ? dateFormat.format(propertyType.getModified()) : "");
        }
        return workbook;
    }

    private Set<String> resolvePropertyTypePrefLabelLanguages(final Set<PropertyTypeDTO> propertyTypes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final PropertyTypeDTO propertyType : propertyTypes) {
            final Map<String, String> prefLabel = propertyType.getPrefLabel();
            languages.addAll(prefLabel.keySet());
        }
        return languages;
    }

    private Set<String> resolvePropertyTypeDefinitionLanguages(final Set<PropertyTypeDTO> propertyTypes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final PropertyTypeDTO propertyType : propertyTypes) {
            final Map<String, String> definition = propertyType.getDefinition();
            languages.addAll(definition.keySet());
        }
        return languages;
    }
}
