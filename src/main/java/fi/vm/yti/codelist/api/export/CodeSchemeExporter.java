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
import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ExtensionDTO;
import fi.vm.yti.codelist.common.dto.ExternalReferenceDTO;
import fi.vm.yti.codelist.common.dto.OrganizationDTO;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
public class CodeSchemeExporter extends BaseExporter {

    private final Domain domain;
    private final CodeExporter codeExporter;
    private final ExtensionExporter extensionExporter;
    private final MemberExporter memberExporter;
    private final ExternalReferenceExporter externalReferenceExporter;

    public CodeSchemeExporter(final Domain domain,
                              final CodeExporter codeExporter,
                              final ExtensionExporter extensionExporter,
                              final MemberExporter memberExporter,
                              final ExternalReferenceExporter externalReferenceExporter) {
        this.domain = domain;
        this.codeExporter = codeExporter;
        this.extensionExporter = extensionExporter;
        this.memberExporter = memberExporter;
        this.externalReferenceExporter = externalReferenceExporter;
    }

    public String createCsv(final CodeSchemeDTO codeScheme) {
        final Set<CodeSchemeDTO> codeSchemes = new HashSet<>();
        codeSchemes.add(codeScheme);
        return createCsv(codeSchemes);
    }

    public String createCsv(final Set<CodeSchemeDTO> codeSchemes) {
        final Set<String> prefLabelLanguages = resolveCodeSchemePrefLabelLanguages(codeSchemes);
        final Set<String> definitionLanguages = resolveCodeSchemeDefinitionLanguages(codeSchemes);
        final Set<String> descriptionLanguages = resolveCodeSchemeDescriptionLanguages(codeSchemes);
        final Set<String> changeNoteLanguages = resolveCodeSchemeChangeNoteLanguages(codeSchemes);
        final Set<String> feedbackChannelLanguages = resolveCodeSchemeFeedbackChannelLanguages(codeSchemes);
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, CONTENT_HEADER_CODEVALUE);
        appendValue(csv, CONTENT_HEADER_URI);
        appendValue(csv, CONTENT_HEADER_ORGANIZATION);
        appendValue(csv, CONTENT_HEADER_INFODOMAIN);
        appendValue(csv, CONTENT_HEADER_LANGUAGECODE);
        appendValue(csv, CONTENT_HEADER_VERSION);
        appendValue(csv, CONTENT_HEADER_STATUS);
        appendValue(csv, CONTENT_HEADER_SOURCE);
        appendValue(csv, CONTENT_HEADER_LEGALBASE);
        appendValue(csv, CONTENT_HEADER_GOVERNANCEPOLICY);
        appendValue(csv, CONTENT_HEADER_CONCEPTURI);
        appendValue(csv, CONTENT_HEADER_DEFAULTCODE);
        prefLabelLanguages.forEach(language -> appendValue(csv, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        definitionLanguages.forEach(language -> appendValue(csv, CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase()));
        descriptionLanguages.forEach(language -> appendValue(csv, CONTENT_HEADER_DESCRIPTION_PREFIX + language.toUpperCase()));
        changeNoteLanguages.forEach(language -> appendValue(csv, CONTENT_HEADER_CHANGENOTE_PREFIX + language.toUpperCase()));
        appendValue(csv, CONTENT_HEADER_STARTDATE);
        appendValue(csv, CONTENT_HEADER_ENDDATE);
        appendValue(csv, CONTENT_HEADER_CREATED);
        appendValue(csv, CONTENT_HEADER_MODIFIED);
        feedbackChannelLanguages.forEach(language -> appendValue(csv, CONTENT_HEADER_FEEDBACK_CHANNEL_PREFIX + language.toUpperCase()));
        appendValue(csv, CONTENT_HEADER_HREF, true);
        for (final CodeSchemeDTO codeScheme : codeSchemes) {
            appendValue(csv, codeScheme.getCodeValue());
            appendValue(csv, codeScheme.getUri());
            appendValue(csv, formatOrganizationsToString(codeScheme.getOrganizations()));
            appendValue(csv, formatCodesToString(codeScheme.getInfoDomains()));
            appendValue(csv, formatCodesToString(codeScheme.getLanguageCodes()));
            appendValue(csv, codeScheme.getVersion());
            appendValue(csv, codeScheme.getStatus());
            appendValue(csv, codeScheme.getSource());
            appendValue(csv, codeScheme.getLegalBase());
            appendValue(csv, codeScheme.getGovernancePolicy());
            appendValue(csv, codeScheme.getConceptUriInVocabularies());
            appendValue(csv, codeScheme.getDefaultCode() != null ? codeScheme.getDefaultCode().getCodeValue() : "");
            prefLabelLanguages.forEach(language -> appendValue(csv, getCodeSchemePrefLabel(codeScheme, language)));
            definitionLanguages.forEach(language -> appendValue(csv, getCodeSchemeDefinition(codeScheme, language)));
            descriptionLanguages.forEach(language -> appendValue(csv, getCodeSchemeDescription(codeScheme, language)));
            changeNoteLanguages.forEach(language -> appendValue(csv, getCodeSchemeChangeNote(codeScheme, language)));
            appendValue(csv, codeScheme.getStartDate() != null ? formatDateWithISO8601(codeScheme.getStartDate()) : "");
            appendValue(csv, codeScheme.getEndDate() != null ? formatDateWithISO8601(codeScheme.getEndDate()) : "");
            appendValue(csv, codeScheme.getCreated() != null ? formatDateWithSeconds(codeScheme.getCreated()) : "");
            appendValue(csv, codeScheme.getModified() != null ? formatDateWithSeconds(codeScheme.getModified()) : "");
            feedbackChannelLanguages.forEach(language -> appendValue(csv, getCodeSchemeFeedbackChannel(codeScheme, language)));
            appendValue(csv, formatExternalReferencesToString(codeScheme.getExternalReferences()),true);
        }
        return csv.toString();
    }

    public Workbook createExcel(final CodeSchemeDTO codeScheme,
                                final String format) {
        final Workbook workbook = createWorkBook(format);
        final Set<CodeSchemeDTO> codeSchemes = new HashSet<>();
        codeSchemes.add(codeScheme);
        addCodeSchemeSheet(workbook, codeSchemes);
        final String externalReferenceSheetName = createLinksSheetName(codeScheme);
        final Set<ExternalReferenceDTO> externalReferences = domain.getExternalReferences(codeScheme);
        externalReferenceExporter.addExternalReferencesSheet(workbook, externalReferenceSheetName, externalReferences);
        final String codeSheetName = createCodesSheetName(codeScheme);
        codeExporter.addCodeSheet(workbook, codeSheetName, domain.getCodesByCodeRegistryCodeValueAndCodeSchemeCodeValue(codeScheme.getCodeRegistry().getCodeValue(), codeScheme.getCodeValue()));
        final Set<ExtensionDTO> extensions = domain.getExtensions(codeScheme);
        final String extensionSheetName = createExtensionsSheetName(codeScheme);
        if (extensions != null && !extensions.isEmpty()) {
            extensionExporter.addExtensionSheet(workbook, extensionSheetName, extensions);
            int i = 0;
            for (final ExtensionDTO extension : extensions) {
                final String memberSheetName = truncateSheetNameWithIndex(EXCEL_SHEET_MEMBERS + "_" + codeScheme.getCodeValue() + "_" + extension.getCodeValue(), ++i);
                memberExporter.addMembersSheet(extension, workbook, memberSheetName, domain.getMembers(extension, null));
            }
        } else {
            extensionExporter.addExtensionSheet(workbook, extensionSheetName, new HashSet<>());
        }
        return workbook;
    }

    public Workbook createExcel(final Set<CodeSchemeDTO> codeSchemes,
                                final String format) {
        final Workbook workbook = createWorkBook(format);
        addCodeSchemeSheet(workbook, codeSchemes);
        return workbook;
    }

    private void addCodeSchemeSheet(final Workbook workbook,
                                    final Set<CodeSchemeDTO> codeSchemes) {
        final Set<String> prefLabelLanguages = resolveCodeSchemePrefLabelLanguages(codeSchemes);
        final Set<String> feedbackChannelLanguages = resolveCodeSchemeFeedbackChannelLanguages(codeSchemes);
        final Set<String> definitionLanguages = resolveCodeSchemeDefinitionLanguages(codeSchemes);
        final Set<String> descriptionLanguages = resolveCodeSchemeDescriptionLanguages(codeSchemes);
        final Set<String> changeNoteLanguages = resolveCodeSchemeChangeNoteLanguages(codeSchemes);
        final Sheet sheet = workbook.createSheet(EXCEL_SHEET_CODESCHEMES);
        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODEVALUE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_URI);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ORGANIZATION);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_INFODOMAIN);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_LANGUAGECODE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_VERSION);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_STATUS);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_SOURCE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_LEGALBASE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_GOVERNANCEPOLICY);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CONCEPTURI);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_DEFAULTCODE);
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
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CREATED);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_MODIFIED);
        for (final String language : feedbackChannelLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_FEEDBACK_CHANNEL_PREFIX + language.toUpperCase());
        }
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_HREF);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODESSHEET);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_LINKSSHEET);
        rowhead.createCell(j).setCellValue(CONTENT_HEADER_EXTENSIONSSHEET);
        int i = 1;
        for (final CodeSchemeDTO codeScheme : codeSchemes) {
            final Row row = sheet.createRow(i++);
            int k = 0;
            row.createCell(k++).setCellValue(checkEmptyValue(codeScheme.getCodeValue()));
            row.createCell(k++).setCellValue(codeScheme.getUri());
            row.createCell(k++).setCellValue(checkEmptyValue(formatOrganizationsToString(codeScheme.getOrganizations())));
            row.createCell(k++).setCellValue(checkEmptyValue(formatCodesToString(codeScheme.getInfoDomains())));
            row.createCell(k++).setCellValue(checkEmptyValue(formatCodesToString(codeScheme.getLanguageCodes())));
            row.createCell(k++).setCellValue(checkEmptyValue(codeScheme.getVersion()));
            row.createCell(k++).setCellValue(checkEmptyValue(codeScheme.getStatus()));
            row.createCell(k++).setCellValue(checkEmptyValue(codeScheme.getSource()));
            row.createCell(k++).setCellValue(checkEmptyValue(codeScheme.getLegalBase()));
            row.createCell(k++).setCellValue(checkEmptyValue(codeScheme.getGovernancePolicy()));
            row.createCell(k++).setCellValue(checkEmptyValue(codeScheme.getConceptUriInVocabularies()));
            row.createCell(k++).setCellValue(checkEmptyValue(codeScheme.getDefaultCode() != null ? codeScheme.getDefaultCode().getCodeValue() : ""));
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(getCodeSchemePrefLabel(codeScheme, language));
            }
            for (final String language : definitionLanguages) {
                row.createCell(k++).setCellValue(getCodeSchemeDefinition(codeScheme, language));
            }
            for (final String language : descriptionLanguages) {
                row.createCell(k++).setCellValue(getCodeSchemeDescription(codeScheme, language));
            }
            for (final String language : changeNoteLanguages) {
                row.createCell(k++).setCellValue(getCodeSchemeChangeNote(codeScheme, language));
            }
            row.createCell(k++).setCellValue(codeScheme.getStartDate() != null ? formatDateWithISO8601(codeScheme.getStartDate()) : "");
            row.createCell(k++).setCellValue(codeScheme.getEndDate() != null ? formatDateWithISO8601(codeScheme.getEndDate()) : "");
            row.createCell(k++).setCellValue(codeScheme.getCreated() != null ? formatDateWithSeconds(codeScheme.getCreated()) : "");
            row.createCell(k++).setCellValue(codeScheme.getModified() != null ? formatDateWithSeconds(codeScheme.getModified()) : "");
            for (final String language : feedbackChannelLanguages) {
                row.createCell(k++).setCellValue(getCodeSchemeFeedbackChannel(codeScheme, language));
            }
            row.createCell(k++).setCellValue(checkEmptyValue(formatExternalReferencesToString(codeScheme.getExternalReferences())));
            row.createCell(k++).setCellValue(checkEmptyValue(createCodesSheetName(codeScheme)));
            row.createCell(k++).setCellValue(checkEmptyValue(createLinksSheetName(codeScheme)));
            row.createCell(k).setCellValue(checkEmptyValue(createExtensionsSheetName(codeScheme)));
        }
    }

    private Set<String> resolveCodeSchemePrefLabelLanguages(final Set<CodeSchemeDTO> codeSchemes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeSchemeDTO codeScheme : codeSchemes) {
            final Map<String, String> prefLabel = codeScheme.getPrefLabel();
            if (prefLabel != null && !prefLabel.isEmpty()) {
                languages.addAll(prefLabel.keySet());
            }
        }
        return languages;
    }

    private Set<String> resolveCodeSchemeFeedbackChannelLanguages(final Set<CodeSchemeDTO> codeSchemes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeSchemeDTO codeScheme : codeSchemes) {
            final Map<String, String> feedbackChannel = codeScheme.getFeedbackChannel();
            if (feedbackChannel != null && !feedbackChannel.isEmpty()) {
                languages.addAll(feedbackChannel.keySet());
            }
        }
        return languages;
    }

    private Set<String> resolveCodeSchemeDefinitionLanguages(final Set<CodeSchemeDTO> codeSchemes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeSchemeDTO codeScheme : codeSchemes) {
            final Map<String, String> definition = codeScheme.getDefinition();
            if (definition != null && !definition.isEmpty()) {
                languages.addAll(definition.keySet());
            }
        }
        return languages;
    }

    private Set<String> resolveCodeSchemeDescriptionLanguages(final Set<CodeSchemeDTO> codeSchemes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeSchemeDTO codeScheme : codeSchemes) {
            final Map<String, String> description = codeScheme.getDescription();
            if (description != null && !description.isEmpty()) {
                languages.addAll(description.keySet());
            }
        }
        return languages;
    }

    private Set<String> resolveCodeSchemeChangeNoteLanguages(final Set<CodeSchemeDTO> codeSchemes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeSchemeDTO codeScheme : codeSchemes) {
            final Map<String, String> changeNote = codeScheme.getChangeNote();
            if (changeNote != null && !changeNote.isEmpty()) {
                languages.addAll(changeNote.keySet());
            }
        }
        return languages;
    }

    private String formatOrganizationsToString(final Set<OrganizationDTO> organizations) {
        final StringBuilder csvOrganizations = new StringBuilder();
        if (organizations != null && !organizations.isEmpty()) {
            int i = 0;
            for (final OrganizationDTO organization : organizations) {
                i++;
                csvOrganizations.append(organization.getId());
                if (i < organizations.size()) {
                    csvOrganizations.append(";");
                }
            }
        }
        return csvOrganizations.toString();
    }

    private String formatCodesToString(final Set<CodeDTO> codes) {
        final StringBuilder csvCodes = new StringBuilder();
        if (codes != null && !codes.isEmpty()) {
            int i = 0;
            for (final CodeDTO code : codes) {
                i++;
                csvCodes.append(code.getCodeValue().trim());
                if (i < codes.size()) {
                    csvCodes.append(";");
                }
            }
        }
        return csvCodes.toString();
    }
}
