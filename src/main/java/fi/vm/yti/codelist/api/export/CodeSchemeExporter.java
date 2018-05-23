package fi.vm.yti.codelist.api.export;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.api.domain.Domain;
import fi.vm.yti.codelist.api.exception.YtiCodeListException;
import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ErrorModel;
import fi.vm.yti.codelist.common.dto.ExtensionSchemeDTO;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
public class CodeSchemeExporter extends BaseExporter {

    private static final Logger LOG = LoggerFactory.getLogger(CodeSchemeExporter.class);

    private Domain domain;
    private CodeExporter codeExporter;
    private ExtensionSchemeExporter extensionSchemeExporter;
    private ExtensionExporter extensionExporter;

    public CodeSchemeExporter(final Domain domain,
                              final CodeExporter codeExporter,
                              final ExtensionSchemeExporter extensionSchemeExporter,
                              final ExtensionExporter extensionExporter) {
        this.domain = domain;

        this.codeExporter = codeExporter;
        this.extensionSchemeExporter = extensionSchemeExporter;
        this.extensionExporter = extensionExporter;
    }

    public String createCsv(final Set<CodeSchemeDTO> codeSchemes) {
        final Set<String> prefLabelLanguages = resolveCodeSchemePrefLabelLanguages(codeSchemes);
        final Set<String> definitionLanguages = resolveCodeSchemeDefinitionLanguages(codeSchemes);
        final Set<String> descriptionLanguages = resolveCodeSchemeDescriptionLanguages(codeSchemes);
        final Set<String> changeNoteLanguages = resolveCodeSchemeChangeNoteLanguages(codeSchemes);
        final DateFormat dateFormat = new SimpleDateFormat(DATEFORMAT);
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, csvSeparator, CONTENT_HEADER_CODEVALUE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ID);
        appendValue(csv, csvSeparator, CONTENT_HEADER_CLASSIFICATION);
        appendValue(csv, csvSeparator, CONTENT_HEADER_VERSION);
        appendValue(csv, csvSeparator, CONTENT_HEADER_STATUS);
        appendValue(csv, csvSeparator, CONTENT_HEADER_SOURCE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_LEGALBASE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_GOVERNANCEPOLICY);
        prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        definitionLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase()));
        descriptionLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_DESCRIPTION_PREFIX + language.toUpperCase()));
        changeNoteLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_CHANGENOTE_PREFIX + language.toUpperCase()));
        appendValue(csv, csvSeparator, CONTENT_HEADER_STARTDATE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ENDDATE, true);
        for (final CodeSchemeDTO codeScheme : codeSchemes) {
            appendValue(csv, csvSeparator, codeScheme.getCodeValue());
            appendValue(csv, csvSeparator, codeScheme.getId().toString());
            appendValue(csv, csvSeparator, formatDataClassificationsToString(codeScheme.getDataClassifications()));
            appendValue(csv, csvSeparator, codeScheme.getVersion());
            appendValue(csv, csvSeparator, codeScheme.getStatus());
            appendValue(csv, csvSeparator, codeScheme.getSource());
            appendValue(csv, csvSeparator, codeScheme.getLegalBase());
            appendValue(csv, csvSeparator, codeScheme.getGovernancePolicy());
            prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, codeScheme.getPrefLabel().get(language)));
            definitionLanguages.forEach(language -> appendValue(csv, csvSeparator, codeScheme.getDefinition().get(language)));
            descriptionLanguages.forEach(language -> appendValue(csv, csvSeparator, codeScheme.getDescription().get(language)));
            changeNoteLanguages.forEach(language -> appendValue(csv, csvSeparator, codeScheme.getChangeNote().get(language)));
            appendValue(csv, csvSeparator, codeScheme.getStartDate() != null ? dateFormat.format(codeScheme.getStartDate()) : "");
            appendValue(csv, csvSeparator, codeScheme.getEndDate() != null ? dateFormat.format(codeScheme.getEndDate()) : "", true);
        }
        return csv.toString();
    }

    public Workbook createExcel(final CodeSchemeDTO codeScheme,
                                final String format) {
        try (final Workbook workbook = createWorkBook(format)) {
            final Set<CodeSchemeDTO> codeSchemes = new HashSet<>();
            codeSchemes.add(codeScheme);
            addCodeSchemeSheet(workbook, EXCEL_SHEET_CODESCHEMES, codeSchemes);
            codeExporter.addCodeSheet(workbook, EXCEL_SHEET_CODES + "_" + codeScheme.getCodeValue(), domain.getCodesByCodeRegistryCodeValueAndCodeSchemeCodeValue(codeScheme.getCodeRegistry().getCodeValue(), codeScheme.getCodeValue()));
            final Set<ExtensionSchemeDTO> extensionSchemes = domain.getExtensionSchemes(null, null, null, codeScheme, null, null);
            if (extensionSchemes != null && !extensionSchemes.isEmpty()) {
                extensionSchemeExporter.addExtensionSchemesSheet(workbook, EXCEL_SHEET_EXTENSIONSCHEMES + "_" + codeScheme.getCodeValue(), extensionSchemes);
                extensionSchemes.forEach(extensionScheme -> extensionExporter.addExtensionsSheet(workbook, EXCEL_SHEET_EXTENSIONS + "_" + codeScheme.getCodeValue() + "_" + extensionScheme.getCodeValue(), domain.getExtensions(null, null, extensionScheme, null, null)));
            }
            return workbook;
        } catch (final IOException e) {
            LOG.error("Error creating Excel during export!", e);
            throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "Excel output generation failed!"));
        }
    }

    public Workbook createExcel(final Set<CodeSchemeDTO> codeSchemes,
                                final String format) {
        try (final Workbook workbook = createWorkBook(format)) {
            addCodeSchemeSheet(workbook, EXCEL_SHEET_CODESCHEMES, codeSchemes);
            return workbook;
        } catch (final IOException e) {
            LOG.error("Error creating Excel during export!", e);
            throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "Excel output generation failed!"));
        }
    }

    private void addCodeSchemeSheet(final Workbook workbook,
                                   final String sheetName,
                                   final Set<CodeSchemeDTO> codeSchemes) {
        final Set<String> prefLabelLanguages = resolveCodeSchemePrefLabelLanguages(codeSchemes);
        final Set<String> definitionLanguages = resolveCodeSchemeDefinitionLanguages(codeSchemes);
        final Set<String> descriptionLanguages = resolveCodeSchemeDescriptionLanguages(codeSchemes);
        final Set<String> changeNoteLanguages = resolveCodeSchemeChangeNoteLanguages(codeSchemes);
        final DateFormat dateFormat = new SimpleDateFormat(DATEFORMAT);
        final Sheet sheet = workbook.createSheet(sheetName);
        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODEVALUE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ID);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CLASSIFICATION);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_VERSION);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_STATUS);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_SOURCE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_LEGALBASE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_GOVERNANCEPOLICY);
        for (final String language : prefLabelLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        }
        for (final String language : definitionLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase());
        }
        for (final String language : descriptionLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_DESCRIPTION_PREFIX + language.toUpperCase());
        }
        for (final String language : changeNoteLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CHANGENOTE_PREFIX + language.toUpperCase());
        }
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_STARTDATE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ENDDATE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODESSHEET);
        rowhead.createCell(j).setCellValue(CONTENT_HEADER_EXTENSIONSCHEMESSHEET);
        int i = 1;
        for (final CodeSchemeDTO codeScheme : codeSchemes) {
            final Row row = sheet.createRow(i++);
            int k = 0;
            row.createCell(k++).setCellValue(checkEmptyValue(codeScheme.getCodeValue()));
            row.createCell(k++).setCellValue(checkEmptyValue(codeScheme.getId().toString()));
            row.createCell(k++).setCellValue(checkEmptyValue(formatDataClassificationsToString(codeScheme.getDataClassifications())));
            row.createCell(k++).setCellValue(checkEmptyValue(codeScheme.getVersion()));
            row.createCell(k++).setCellValue(checkEmptyValue(codeScheme.getStatus()));
            row.createCell(k++).setCellValue(checkEmptyValue(codeScheme.getSource()));
            row.createCell(k++).setCellValue(checkEmptyValue(codeScheme.getLegalBase()));
            row.createCell(k++).setCellValue(checkEmptyValue(codeScheme.getGovernancePolicy()));
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(codeScheme.getPrefLabel().get(language));
            }
            for (final String language : definitionLanguages) {
                row.createCell(k++).setCellValue(codeScheme.getDefinition().get(language));
            }
            for (final String language : descriptionLanguages) {
                row.createCell(k++).setCellValue(codeScheme.getDescription().get(language));
            }
            for (final String language : changeNoteLanguages) {
                row.createCell(k++).setCellValue(codeScheme.getChangeNote().get(language));
            }
            row.createCell(k++).setCellValue(codeScheme.getStartDate() != null ? dateFormat.format(codeScheme.getStartDate()) : "");
            row.createCell(k++).setCellValue(codeScheme.getEndDate() != null ? dateFormat.format(codeScheme.getEndDate()) : "");
            row.createCell(k++).setCellValue(checkEmptyValue(createCodesSheetName(codeScheme)));
            row.createCell(k).setCellValue(checkEmptyValue(createExtensionSchemesSheetName(codeScheme)));
        }
    }

    private Set<String> resolveCodeSchemePrefLabelLanguages(final Set<CodeSchemeDTO> codeSchemes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeSchemeDTO codeScheme : codeSchemes) {
            final Map<String, String> prefLabel = codeScheme.getPrefLabel();
            languages.addAll(prefLabel.keySet());
        }
        return languages;
    }

    private Set<String> resolveCodeSchemeDefinitionLanguages(final Set<CodeSchemeDTO> codeSchemes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeSchemeDTO codeScheme : codeSchemes) {
            final Map<String, String> definition = codeScheme.getDefinition();
            languages.addAll(definition.keySet());
        }
        return languages;
    }

    private Set<String> resolveCodeSchemeDescriptionLanguages(final Set<CodeSchemeDTO> codeSchemes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeSchemeDTO codeScheme : codeSchemes) {
            final Map<String, String> description = codeScheme.getDescription();
            languages.addAll(description.keySet());
        }
        return languages;
    }

    private Set<String> resolveCodeSchemeChangeNoteLanguages(final Set<CodeSchemeDTO> codeSchemes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeSchemeDTO codeScheme : codeSchemes) {
            final Map<String, String> changeNote = codeScheme.getChangeNote();
            languages.addAll(changeNote.keySet());
        }
        return languages;
    }

    private String formatDataClassificationsToString(final Set<CodeDTO> classifications) {
        final StringBuilder csvClassifications = new StringBuilder();
        int i = 0;
        for (final CodeDTO code : classifications) {
            i++;
            csvClassifications.append(code.getCodeValue().trim());
            if (i < classifications.size()) {
                csvClassifications.append(";");
            }
            i++;
        }
        return csvClassifications.toString();
    }

    private String createExtensionSchemesSheetName(final CodeSchemeDTO codeScheme) {
        return "ExtensionSchemes_" + codeScheme.getCodeValue();
    }

    private String createCodesSheetName(final CodeSchemeDTO codeScheme) {
        return "Codes_" + codeScheme.getCodeValue();
    }
}
