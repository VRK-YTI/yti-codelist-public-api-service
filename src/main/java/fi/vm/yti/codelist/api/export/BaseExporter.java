package fi.vm.yti.codelist.api.export;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.Set;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ExternalReferenceDTO;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
abstract class BaseExporter {

    private static final String DATEFORMAT = "yyyy-MM-dd";
    private static final String DATEFORMAT_WITH_SECONDS = "yyyy-MM-dd HH:mm:ss";
    private static final int MAX_SHEETNAME_SIZE = 31;

    String checkEmptyValue(final String value) {
        if (value == null) {
            return "";
        }
        return value;
    }

    void appendValue(final StringBuilder builder,
                     final String separator,
                     final String value) {
        appendValue(builder, separator, value, false);
    }

    void appendValue(final StringBuilder builder,
                     final String separator,
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
            builder.append(separator);
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

    String createExternalReferencesSheetName(final CodeSchemeDTO codeScheme) {
        return truncateSheetName(EXCEL_SHEET_EXTERNALREFERENCES + "_" + codeScheme.getCodeValue());
    }

    String truncateSheetName(final String sheetName) {
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
        final DateFormat dateFormat = new SimpleDateFormat(DATEFORMAT);
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
}
