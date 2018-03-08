package fi.vm.yti.codelist.api.export;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Component;

import static fi.vm.yti.codelist.common.constants.ApiConstants.FORMAT_EXCEL_XLSX;

@Component
abstract class BaseExporter {

    static final String DATEFORMAT = "yyyy-MM-dd";

    String checkEmptyValue(final String value) {
        if (value == null) {
            return "";
        }
        return value;
    }

    void appendValue(final StringBuilder builder, final String separator, final String value) {
        appendValue(builder, separator, value, false);
    }

    void appendValue(final StringBuilder builder, final String separator, final String value, final boolean isLast) {
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
        if (FORMAT_EXCEL_XLSX.equals(format)) {
            return new XSSFWorkbook();
        } else {
            return new HSSFWorkbook();
        }
    }
}
