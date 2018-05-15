package fi.vm.yti.codelist.api.export;

import java.io.IOException;
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
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, csvSeparator, CONTENT_HEADER_ID);
        appendValue(csv, csvSeparator, CONTENT_HEADER_EXTENSIONVALUE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_CODE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_RELATION);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ORDER, true);
        for (final ExtensionDTO extension : extensions) {
            appendValue(csv, csvSeparator, extension.getId().toString());
            appendValue(csv, csvSeparator, extension.getExtensionValue());
            appendValue(csv, csvSeparator, extension.getCode() != null ? extension.getCode().getCodeValue() : "");
            appendValue(csv, csvSeparator, extension.getExtension() != null ? extension.getExtension().getExtensionValue() : "");
            appendValue(csv, csvSeparator, extension.getOrder().toString(), true);
        }
        return csv.toString();
    }

    public Workbook createExcel(final Set<ExtensionDTO> extensions,
                                final String format) {
        try (final Workbook workbook = createWorkBook(format)) {
            final Sheet sheet = workbook.createSheet(EXCEL_SHEET_EXTENSIONS);
            final Row rowhead = sheet.createRow((short) 0);
            int j = 0;
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ID);
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_EXTENSIONVALUE);
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODE);
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_RELATION);
            rowhead.createCell(j).setCellValue(CONTENT_HEADER_ORDER);
            int i = 1;
            for (final ExtensionDTO extension : extensions) {
                final Row row = sheet.createRow(i++);
                int k = 0;
                row.createCell(k++).setCellValue(checkEmptyValue(extension.getId().toString()));
                row.createCell(k++).setCellValue(checkEmptyValue(extension.getExtensionValue()));
                if (extension.getCode() != null) {
                    row.createCell(k++).setCellValue(checkEmptyValue(extension.getCode().getCodeValue()));
                } else {
                    row.createCell(k++).setCellValue("");
                }
                if (extension.getExtension() != null) {
                    row.createCell(k++).setCellValue(checkEmptyValue(extension.getExtension().getExtensionValue()));
                } else {
                    row.createCell(k++).setCellValue("");
                }
                row.createCell(k).setCellValue(checkEmptyValue(extension.getOrder().toString()));
            }
            return workbook;
        } catch (final IOException e) {
            LOG.error("Error creating Excel during export!", e);
            throw new YtiCodeListException(new ErrorModel(HttpStatus.INTERNAL_SERVER_ERROR.value(), "Excel output generation failed!"));
        }
    }
}
