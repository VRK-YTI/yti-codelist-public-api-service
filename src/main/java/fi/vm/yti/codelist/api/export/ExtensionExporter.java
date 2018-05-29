package fi.vm.yti.codelist.api.export;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Set;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.api.exception.YtiCodeListException;
import fi.vm.yti.codelist.common.dto.ErrorModel;
import fi.vm.yti.codelist.common.dto.ExtensionDTO;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
public class ExtensionExporter extends BaseExporter {

    private static final Logger LOG = LoggerFactory.getLogger(ExtensionExporter.class);

    public String createCsv(final Set<ExtensionDTO> extensions) {
        final DateFormat dateFormat = new SimpleDateFormat(DATEFORMAT);
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, csvSeparator, CONTENT_HEADER_ID);
        appendValue(csv, csvSeparator, CONTENT_HEADER_EXTENSIONVALUE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_CODE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_RELATION);
        appendValue(csv, csvSeparator, CONTENT_HEADER_CREATED);
        appendValue(csv, csvSeparator, CONTENT_HEADER_MODIFIED);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ORDER, true);
        for (final ExtensionDTO extension : extensions) {
            appendValue(csv, csvSeparator, extension.getId().toString());
            appendValue(csv, csvSeparator, extension.getExtensionValue());
            appendValue(csv, csvSeparator, extension.getCode() != null ? extension.getCode().getCodeValue() : "");
            appendValue(csv, csvSeparator, extension.getExtension() != null ? extension.getExtension().getCode().getCodeValue() : "");
            appendValue(csv, csvSeparator, extension.getCreated() != null ? dateFormat.format(extension.getExtension().getCode().getCreated()) : "");
            appendValue(csv, csvSeparator, extension.getModified() != null ? dateFormat.format(extension.getExtension().getCode().getModified()) : "");
            appendValue(csv, csvSeparator, extension.getOrder().toString(), true);
        }
        return csv.toString();
    }

    public Workbook createExcel(final Set<ExtensionDTO> extensions,
                                final String format) {
        final Workbook workbook = createWorkBook(format);
        addExtensionsSheet(workbook, EXCEL_SHEET_EXTENSIONS, extensions);
        return workbook;
    }

    public void addExtensionsSheet(final Workbook workbook,
                                    final String sheetName,
                                    final Set<ExtensionDTO> extensions) {
        final DateFormat dateFormat = new SimpleDateFormat(DATEFORMAT);
        final Sheet sheet = workbook.createSheet(sheetName);
        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ID);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_EXTENSIONVALUE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_RELATION);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CREATED);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_MODIFIED);
        rowhead.createCell(j).setCellValue(CONTENT_HEADER_ORDER);
        int i = 1;
        for (final ExtensionDTO extension : extensions) {
            final Row row = sheet.createRow(i++);
            int k = 0;
            row.createCell(k++).setCellValue(checkEmptyValue(extension.getId().toString()));
            row.createCell(k++).setCellValue(checkEmptyValue(extension.getExtensionValue()));
            if (extension.getCode() != null) {
                row.createCell(k++).setCellValue(checkEmptyValue(extension.getCode().getUri()));
            } else {
                row.createCell(k++).setCellValue("");
            }
            if (extension.getExtension() != null && extension.getExtension().getCode() != null) {
                row.createCell(k++).setCellValue(checkEmptyValue(extension.getExtension().getCode().getUri()));
            } else {
                row.createCell(k++).setCellValue("");
            }
            row.createCell(k++).setCellValue(extension.getCreated() != null ? dateFormat.format(extension.getCreated()) : "");
            row.createCell(k++).setCellValue(extension.getModified() != null ? dateFormat.format(extension.getModified()) : "");
            row.createCell(k).setCellValue(checkEmptyValue(extension.getOrder().toString()));
        }
    }
}
