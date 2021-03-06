package fi.vm.yti.codelist.api.export;

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
        final Set<String> prefLabelLanguages = resolvePropertyTypePrefLabelLanguages(propertyTypes);
        final Set<String> definitionLanguages = resolvePropertyTypeDefinitionLanguages(propertyTypes);
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, CONTENT_HEADER_LOCALNAME);
        appendValue(csv, CONTENT_HEADER_ID);
        appendValue(csv, CONTENT_HEADER_URI);
        appendValue(csv, CONTENT_HEADER_CONTEXT);
        prefLabelLanguages.forEach(language -> appendValue(csv, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        definitionLanguages.forEach(language -> appendValue(csv, CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase()));
        appendValue(csv, CONTENT_HEADER_CREATED);
        appendValue(csv, CONTENT_HEADER_MODIFIED, true);
        csv.append("\n");
        for (final PropertyTypeDTO propertyType : propertyTypes) {
            appendValue(csv, propertyType.getLocalName());
            appendValue(csv, propertyType.getId().toString());
            appendValue(csv, propertyType.getUri());
            appendValue(csv, propertyType.getContext());
            prefLabelLanguages.forEach(language -> appendValue(csv, getPropertyTypePrefLabel(propertyType, language)));
            definitionLanguages.forEach(language -> appendValue(csv, getPropertyTypeDefinition(propertyType, language)));
            appendValue(csv, propertyType.getCreated() != null ? formatDateWithSeconds(propertyType.getCreated()) : "");
            appendValue(csv, propertyType.getModified() != null ? formatDateWithSeconds(propertyType.getModified()) : "", true);
            csv.append("\n");
        }
        return csv.toString();
    }

    public Workbook createExcel(final Set<PropertyTypeDTO> propertyTypes,
                                final String format) {
        final Workbook workbook = createWorkBook(format);
        final Set<String> prefLabelLanguages = resolvePropertyTypePrefLabelLanguages(propertyTypes);
        final Set<String> definitionLanguages = resolvePropertyTypeDefinitionLanguages(propertyTypes);
        final Sheet sheet = workbook.createSheet(EXCEL_SHEET_PROPERTYTYPES);
        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ID);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_LOCALNAME);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_URI);
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
            row.createCell(k++).setCellValue(checkEmptyValue(propertyType.getUri()));
            row.createCell(k++).setCellValue(checkEmptyValue(propertyType.getContext()));
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(getPropertyTypePrefLabel(propertyType, language));
            }
            for (final String language : definitionLanguages) {
                row.createCell(k++).setCellValue(getPropertyTypeDefinition(propertyType, language));
            }
            row.createCell(k++).setCellValue(propertyType.getCreated() != null ? formatDateWithSeconds(propertyType.getCreated()) : "");
            row.createCell(k).setCellValue(propertyType.getModified() != null ? formatDateWithSeconds(propertyType.getModified()) : "");
        }
        return workbook;
    }

    private Set<String> resolvePropertyTypePrefLabelLanguages(final Set<PropertyTypeDTO> propertyTypes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final PropertyTypeDTO propertyType : propertyTypes) {
            final Map<String, String> prefLabel = propertyType.getPrefLabel();
            if (prefLabel != null && !prefLabel.isEmpty()) {
                languages.addAll(prefLabel.keySet());
            }
        }
        return languages;
    }

    private Set<String> resolvePropertyTypeDefinitionLanguages(final Set<PropertyTypeDTO> propertyTypes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final PropertyTypeDTO propertyType : propertyTypes) {
            final Map<String, String> definition = propertyType.getDefinition();
            if (definition != null && !definition.isEmpty()) {
                languages.addAll(definition.keySet());
            }
        }
        return languages;
    }
}
