package fi.vm.yti.codelist.api.export;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.common.model.CodeRegistry;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
public class CodeRegistryExporter extends BaseExporter {

    public String createCsv(final Set<CodeRegistry> registries) {
        final Set<String> prefLabelLanguages = resolveCodeRegistryPrefLabelLanguages(registries);
        final Set<String> definitionLanguages = resolveCodeRegistryDefinitionLanguages(registries);
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, csvSeparator, CONTENT_HEADER_CODEVALUE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ID);
        prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        definitionLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase()));
        csv.append("\n");
        for (final CodeRegistry codeRegistry : registries) {
            appendValue(csv, csvSeparator, codeRegistry.getCodeValue());
            appendValue(csv, csvSeparator, codeRegistry.getId().toString());
            prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, codeRegistry.getPrefLabel().get(language)));
            definitionLanguages.forEach(language -> appendValue(csv, csvSeparator, codeRegistry.getDefinition().get(language)));
            csv.append("\n");
        }
        return csv.toString();
    }

    public Workbook createExcel(final Set<CodeRegistry> registries,
                                final String format) {
        final Workbook workbook = createWorkBook(format);
        final Set<String> prefLabelLanguages = resolveCodeRegistryPrefLabelLanguages(registries);
        final Set<String> definitionLanguages = resolveCodeRegistryDefinitionLanguages(registries);
        final Sheet sheet = workbook.createSheet(EXCEL_SHEET_CODEREGISTRIES);
        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODEVALUE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ID);
        for (final String language : prefLabelLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        }
        for (final String language : definitionLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase());
        }
        int i = 1;
        for (final CodeRegistry codeRegistry : registries) {
            final Row row = sheet.createRow(i++);
            int k = 0;
            row.createCell(k++).setCellValue(checkEmptyValue(codeRegistry.getCodeValue()));
            row.createCell(k++).setCellValue(checkEmptyValue(codeRegistry.getId().toString()));
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(codeRegistry.getPrefLabel().get(language));
            }
            for (final String language : definitionLanguages) {
                row.createCell(k++).setCellValue(codeRegistry.getDefinition().get(language));
            }
        }
        return workbook;
    }

    private Set<String> resolveCodeRegistryPrefLabelLanguages(final Set<CodeRegistry> registries) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeRegistry registry : registries) {
            final Map<String, String> prefLabel = registry.getPrefLabel();
            languages.addAll(prefLabel.keySet());
        }
        return languages;
    }

    private Set<String> resolveCodeRegistryDefinitionLanguages(final Set<CodeRegistry> registries) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeRegistry registry : registries) {
            final Map<String, String> definition = registry.getDefinition();
            languages.addAll(definition.keySet());
        }
        return languages;
    }
}
