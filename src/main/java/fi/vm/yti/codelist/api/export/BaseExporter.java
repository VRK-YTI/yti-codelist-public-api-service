package fi.vm.yti.codelist.api.export;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.CodeRegistryDTO;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ExtensionDTO;
import fi.vm.yti.codelist.common.dto.ExternalReferenceDTO;
import fi.vm.yti.codelist.common.dto.MemberDTO;
import fi.vm.yti.codelist.common.dto.PropertyTypeDTO;
import fi.vm.yti.codelist.common.dto.ValueTypeDTO;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
abstract class BaseExporter {

    private static final String DATEFORMAT_WITH_SECONDS = "yyyy-MM-dd HH:mm:ss";
    private static final int MAX_SHEETNAME_SIZE = 31;

    String checkEmptyValue(final String value) {
        if (value == null) {
            return "";
        }
        return value;
    }

    void appendValue(final StringBuilder builder,
                     final String value) {
        appendValue(builder, value, false);
    }

    void appendValue(final StringBuilder builder,
                     final String value,
                     final boolean isLast) {
        if (value != null && (value.contains(",") || value.contains("\n"))) {
            builder.append("\"");
            builder.append(checkEmptyValue(value));
            builder.append("\"");
        } else {
            builder.append(checkEmptyValue(value));
        }
        if (isLast) {
            builder.append("\n");
        } else {
            builder.append(",");
        }
    }

    Workbook createWorkBook(final String format) {
        if (FORMAT_EXCEL_XLS.equals(format)) {
            return new HSSFWorkbook();
        } else {
            return new XSSFWorkbook();
        }
    }

    String createExtensionsSheetName(final CodeSchemeDTO codeScheme) {
        return truncateSheetName(EXCEL_SHEET_EXTENSIONS + "_" + codeScheme.getCodeValue());
    }

    String createCodesSheetName(final CodeSchemeDTO codeScheme) {
        return truncateSheetName(EXCEL_SHEET_CODES + "_" + codeScheme.getCodeValue());
    }

    String createLinksSheetName(final CodeSchemeDTO codeScheme) {
        return truncateSheetName(EXCEL_SHEET_LINKS + "_" + codeScheme.getCodeValue());
    }

    private String truncateSheetName(final String sheetName) {
        if (sheetName.length() <= MAX_SHEETNAME_SIZE) {
            return sheetName;
        } else {
            return sheetName.substring(0, MAX_SHEETNAME_SIZE);
        }
    }

    String truncateSheetNameWithIndex(final String sheetName,
                                      final int i) {
        if (sheetName.length() <= MAX_SHEETNAME_SIZE) {
            return sheetName;
        } else {
            return sheetName.substring(0, 26) + "_" + i;
        }
    }

    String formatDateWithISO8601(final LocalDate date) {
        return date.toString();
    }

    String formatDateWithSeconds(final Date date) {
        final DateFormat dateFormat = new SimpleDateFormat(DATEFORMAT_WITH_SECONDS);
        return dateFormat.format(date);
    }

    String formatExternalReferencesToString(final Set<ExternalReferenceDTO> externalReferences) {
        final StringBuilder csvExternalReferences = new StringBuilder();
        if (externalReferences != null && !externalReferences.isEmpty()) {
            int i = 0;
            for (final ExternalReferenceDTO externalReference : externalReferences) {
                i++;
                csvExternalReferences.append(externalReference.getHref());
                if (i < externalReferences.size()) {
                    csvExternalReferences.append("|");
                }
            }
        }
        return csvExternalReferences.toString();
    }

    Set<String> resolveCodePrefLabelLanguages(final Set<CodeDTO> codes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeDTO code : codes) {
            final Map<String, String> prefLabel = code.getPrefLabel();
            if (prefLabel != null) {
                languages.addAll(prefLabel.keySet());
            }
        }
        return languages;
    }

    String getCodeRegistryPrefLabel(final CodeRegistryDTO codeRegistry,
                                    final String language) {
        return codeRegistry.getPrefLabel() != null ? codeRegistry.getPrefLabel().get(language) : "";
    }

    String getCodeRegistryDescription(final CodeRegistryDTO codeRegistry,
                                      final String language) {
        return codeRegistry.getDescription() != null ? codeRegistry.getDescription().get(language) : "";
    }

    String getCodeSchemePrefLabel(final CodeSchemeDTO codeScheme,
                                  final String language) {
        return codeScheme.getPrefLabel() != null ? codeScheme.getPrefLabel().get(language) : "";
    }

    String getCodeSchemeDefinition(final CodeSchemeDTO codeScheme,
                                   final String language) {
        return codeScheme.getDefinition() != null ? codeScheme.getDefinition().get(language) : "";
    }

    String getCodeSchemeDescription(final CodeSchemeDTO codeScheme,
                                    final String language) {
        return codeScheme.getDescription() != null ? codeScheme.getDescription().get(language) : "";
    }

    String getCodeSchemeChangeNote(final CodeSchemeDTO codeScheme,
                                   final String language) {
        return codeScheme.getChangeNote() != null ? codeScheme.getChangeNote().get(language) : "";
    }

    String getCodePrefLabel(final CodeDTO code,
                            final String language) {
        return code.getPrefLabel() != null ? code.getPrefLabel().get(language) : "";
    }

    String getCodeDefinition(final CodeDTO code,
                             final String language) {
        return code.getDefinition() != null ? code.getDefinition().get(language) : "";
    }

    String getCodeDescription(final CodeDTO code,
                              final String language) {
        return code.getDescription() != null ? code.getDescription().get(language) : "";
    }

    String getMemberPrefLabel(final MemberDTO member,
                              final String language) {
        return member.getPrefLabel() != null ? member.getPrefLabel().get(language) : "";
    }

    String getExtensionPrefLabel(final ExtensionDTO extension,
                                 final String language) {
        return extension.getPrefLabel() != null ? extension.getPrefLabel().get(language) : "";
    }

    String getPropertyTypePrefLabel(final PropertyTypeDTO propertyType,
                                    final String language) {
        return propertyType.getPrefLabel() != null ? propertyType.getPrefLabel().get(language) : "";
    }

    String getPropertyTypeDefinition(final PropertyTypeDTO propertyType,
                                     final String language) {
        return propertyType.getDefinition() != null ? propertyType.getDefinition().get(language) : "";
    }

    String getValueTypePrefLabel(final ValueTypeDTO valueType,
                                 final String language) {
        return valueType.getPrefLabel() != null ? valueType.getPrefLabel().get(language) : "";
    }

    String getExternalReferenceTitle(final ExternalReferenceDTO externalReference,
                                     final String language) {
        return externalReference.getTitle() != null ? externalReference.getTitle().get(language) : "";
    }

    String getExternalReferenceDescription(final ExternalReferenceDTO externalReference,
                                           final String language) {
        return externalReference.getDescription() != null ? externalReference.getDescription().get(language) : "";
    }
}
