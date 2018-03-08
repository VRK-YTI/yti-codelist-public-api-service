package fi.vm.yti.codelist.api.export;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.common.model.ExternalReference;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
public class ExternalReferenceExporter extends BaseExporter {

    public String createCsv(final Set<ExternalReference> externalReferences) {
        final Set<String> titleLanguages = resolveExternalReferenceTitleLanguages(externalReferences);
        final Set<String> descriptionLanguages = resolveExternalReferenceDescriptionLanguages(externalReferences);
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, csvSeparator, CONTENT_HEADER_ID);
        titleLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        descriptionLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase()));
        csv.append("\n");
        for (final ExternalReference externalReference : externalReferences) {
            appendValue(csv, csvSeparator, externalReference.getId().toString());
            titleLanguages.forEach(language -> appendValue(csv, csvSeparator, externalReference.getTitle().get(language)));
            descriptionLanguages.forEach(language -> appendValue(csv, csvSeparator, externalReference.getDescription().get(language)));
            csv.append("\n");
        }
        return csv.toString();
    }

    public Workbook createExcel(final Set<ExternalReference> externalReferences,
                                final String format) {
        final Workbook workbook = createWorkBook(format);
        final Set<String> titleLanguages = resolveExternalReferenceTitleLanguages(externalReferences);
        final Set<String> descriptionLanguages = resolveExternalReferenceDescriptionLanguages(externalReferences);
        final Sheet sheet = workbook.createSheet(EXCEL_SHEET_EXTERNALREFERENCES);
        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ID);
        for (final String language : titleLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_TITLE_PREFIX + language.toUpperCase());
        }
        for (final String language : descriptionLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_DESCRIPTION_PREFIX + language.toUpperCase());
        }
        int i = 1;
        for (final ExternalReference externalReference : externalReferences) {
            final Row row = sheet.createRow(i++);
            int k = 0;
            row.createCell(k++).setCellValue(checkEmptyValue(externalReference.getId().toString()));
            for (final String language : titleLanguages) {
                row.createCell(k++).setCellValue(externalReference.getTitle().get(language));
            }
            for (final String language : descriptionLanguages) {
                row.createCell(k++).setCellValue(externalReference.getDescription().get(language));
            }
        }
        return workbook;
    }

    private Set<String> resolveExternalReferenceTitleLanguages(final Set<ExternalReference> externalReferences) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final ExternalReference externalReference : externalReferences) {
            final Map<String, String> title = externalReference.getTitle();
            languages.addAll(title.keySet());
        }
        return languages;
    }

    private Set<String> resolveExternalReferenceDescriptionLanguages(final Set<ExternalReference> externalReferences) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final ExternalReference propertyType : externalReferences) {
            final Map<String, String> description = propertyType.getDescription();
            languages.addAll(description.keySet());
        }
        return languages;
    }
}
