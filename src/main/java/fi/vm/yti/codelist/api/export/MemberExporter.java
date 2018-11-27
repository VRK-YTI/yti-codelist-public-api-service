package fi.vm.yti.codelist.api.export;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import fi.vm.yti.codelist.api.exception.YtiCodeListException;
import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.ErrorModel;
import fi.vm.yti.codelist.common.dto.ExtensionDTO;
import fi.vm.yti.codelist.common.dto.MemberDTO;
import fi.vm.yti.codelist.common.dto.MemberValueDTO;
import fi.vm.yti.codelist.common.dto.ValueTypeDTO;
import static fi.vm.yti.codelist.api.exception.ErrorConstants.ERR_MSG_USER_406;
import static fi.vm.yti.codelist.common.constants.ApiConstants.*;

@Component
public class MemberExporter extends BaseExporter {

    public String createCsv(final ExtensionDTO extension,
                            final Set<MemberDTO> members) {
        final Set<String> prefLabelLanguages = resolveMemberPrefLabelLanguages(members);
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        final Set<ValueTypeDTO> valueTypes = extension != null ? extension.getPropertyType().getValueTypes() : null;
        if (valueTypes != null && !valueTypes.isEmpty()) {
            valueTypes.forEach(valueType -> appendValue(csv, csvSeparator, valueType.getLocalName().toUpperCase()));
        }
        prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        appendValue(csv, csvSeparator, CONTENT_HEADER_ID);
        appendValue(csv, csvSeparator, CONTENT_HEADER_CODE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_RELATION);
        appendValue(csv, csvSeparator, CONTENT_HEADER_STARTDATE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ENDDATE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_CREATED);
        appendValue(csv, csvSeparator, CONTENT_HEADER_MODIFIED);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ORDER, true);
        for (final MemberDTO member : members) {
            if (valueTypes != null && !valueTypes.isEmpty()) {
                valueTypes.forEach(valueType -> {
                    final MemberValueDTO memberValue = member.getMemberValueWithLocalName(valueType.getLocalName());
                    if (memberValue != null) {
                        appendValue(csv, csvSeparator, member.getMemberValueWithLocalName(valueType.getLocalName()).getValue());
                    } else {
                        appendValue(csv, csvSeparator, "");
                    }
                });
            }
            prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, member.getPrefLabel().get(language)));
            appendValue(csv, csvSeparator, member.getId().toString());
            appendValue(csv, csvSeparator, member.getCode() != null ? member.getCode().getUri() : "");
            appendValue(csv, csvSeparator, resolveRelatedMemberIdentifier(members, member.getRelatedMember()));
            appendValue(csv, csvSeparator, member.getStartDate() != null ? formatDateWithISO8601(member.getStartDate()) : "");
            appendValue(csv, csvSeparator, member.getEndDate() != null ? formatDateWithISO8601(member.getEndDate()) : "");
            appendValue(csv, csvSeparator, member.getCreated() != null ? formatDateWithSeconds(member.getCreated()) : "");
            appendValue(csv, csvSeparator, member.getModified() != null ? formatDateWithSeconds(member.getModified()) : "");
            appendValue(csv, csvSeparator, member.getOrder().toString(), true);
        }
        return csv.toString();
    }

    public String createSimplifiedCsvForCrossReferenceList(final ExtensionDTO extension,
                            final Set<MemberDTO> members) {
        Set<CodeDTO> codesInMembers = members.stream().map(m -> m.getCode()).collect(Collectors.toSet());
        final Set<String> prefLabelLanguages = resolveCodePrefLabelLanguages(codesInMembers);
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        final Set<ValueTypeDTO> valueTypes = extension != null ? extension.getPropertyType().getValueTypes() : null;
        if (valueTypes != null && !valueTypes.isEmpty()) {
            valueTypes.forEach(valueType -> appendValue(csv, csvSeparator, valueType.getLocalName().toUpperCase()));
        }
        prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        appendValue(csv, csvSeparator, CONTENT_HEADER_CODEVALUE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_URI1);
        appendValue(csv, csvSeparator, CONTENT_HEADER_URI2, true);
        for (final MemberDTO member : members) {
            if (member.getRelatedMember() == null) {
                continue;
            }
            if (valueTypes != null && !valueTypes.isEmpty()) {
                valueTypes.forEach(valueType -> {
                    final MemberValueDTO memberValue = member.getMemberValueWithLocalName(valueType.getLocalName());
                    if (memberValue != null) {
                        appendValue(csv, csvSeparator, member.getMemberValueWithLocalName(valueType.getLocalName()).getValue());
                    } else {
                        appendValue(csv, csvSeparator, "");
                    }
                });
            }
            prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, member.getCode().getPrefLabel().get(language)));
            appendValue(csv, csvSeparator, member.getCode() != null ? member.getCode().getCodeValue() : "");
            appendValue(csv, csvSeparator, member.getCode() != null ? member.getCode().getUri() : "");
            appendValue(csv, csvSeparator, member.getRelatedMember().getCode().getUri(), true);
        }
        return csv.toString();
    }

    public void addMembersSheet(final ExtensionDTO extension,
                                final Workbook workbook,
                                final String sheetName,
                                final Set<MemberDTO> members) {
        final Set<String> prefLabelLanguages = resolveMemberPrefLabelLanguages(members);
        final Sheet sheet = workbook.createSheet(sheetName);

        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ID);
        final Set<ValueTypeDTO> valueTypes = extension != null ? extension.getPropertyType().getValueTypes() : null;
        if (valueTypes != null && !valueTypes.isEmpty()) {
            for (final ValueTypeDTO valueType : valueTypes) {
                rowhead.createCell(j++).setCellValue(valueType.getLocalName().toUpperCase());
            }
        }
        for (final String language : prefLabelLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        }
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_RELATION);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_STARTDATE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ENDDATE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CREATED);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_MODIFIED);
        rowhead.createCell(j).setCellValue(CONTENT_HEADER_ORDER);
        int i = 1;
        for (final MemberDTO member : members) {
            final Row row = sheet.createRow(i++);
            int k = 0;
            row.createCell(k++).setCellValue(checkEmptyValue(member.getId().toString()));
            if (valueTypes != null && !valueTypes.isEmpty()) {
                for (final ValueTypeDTO valueType : valueTypes) {
                    final MemberValueDTO memberValue = member.getMemberValueWithLocalName(valueType.getLocalName());
                    if (memberValue != null) {
                        row.createCell(k++).setCellValue(checkEmptyValue(memberValue.getValue()));
                    } else {
                        row.createCell(k++).setCellValue("");
                    }
                }
            }
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(member.getPrefLabel().get(language));
            }
            if (member.getCode() != null) {
                row.createCell(k++).setCellValue(checkEmptyValue(member.getCode().getUri()));
            } else {
                row.createCell(k++).setCellValue("");
            }
            row.createCell(k++).setCellValue(resolveRelatedMemberIdentifier(members, member.getRelatedMember()));
            row.createCell(k++).setCellValue(member.getStartDate() != null ? formatDateWithISO8601(member.getStartDate()) : "");
            row.createCell(k++).setCellValue(member.getEndDate() != null ? formatDateWithISO8601(member.getEndDate()) : "");
            row.createCell(k++).setCellValue(member.getCreated() != null ? formatDateWithSeconds(member.getCreated()) : "");
            row.createCell(k++).setCellValue(member.getModified() != null ? formatDateWithSeconds(member.getModified()) : "");
            row.createCell(k).setCellValue(checkEmptyValue(member.getOrder() != null ? member.getOrder().toString() : ""));
        }
    }

    public void addMembersSheetWithCrossRerefences(final ExtensionDTO extension,
                                final Workbook workbook,
                                final String sheetName,
                                final Set<MemberDTO> members) {
        Set<CodeDTO> codesInMembers = members.stream().map(m -> m.getCode()).collect(Collectors.toSet());
        final Set<String> prefLabelLanguages = resolveCodePrefLabelLanguages(codesInMembers);
        final Sheet sheet = workbook.createSheet(sheetName);

        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;

        final Set<ValueTypeDTO> valueTypes = extension != null ? extension.getPropertyType().getValueTypes() : null;
        if (valueTypes != null && !valueTypes.isEmpty()) {
            for (final ValueTypeDTO valueType : valueTypes) {
                rowhead.createCell(j++).setCellValue(valueType.getLocalName().toUpperCase());
            }
        }
        for (final String language : prefLabelLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        }

        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODEVALUE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_URI1);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_URI2);

        int i = 1;
        for (final MemberDTO member : members) {
            if (member.getRelatedMember() == null) {
                continue;
            }
            final Row row = sheet.createRow(i++);
            int k = 0;

            if (valueTypes != null && !valueTypes.isEmpty()) {
                for (final ValueTypeDTO valueType : valueTypes) {
                    final MemberValueDTO memberValue = member.getMemberValueWithLocalName(valueType.getLocalName());
                    if (memberValue != null) {
                        row.createCell(k++).setCellValue(checkEmptyValue(memberValue.getValue()));
                    } else {
                        row.createCell(k++).setCellValue("");
                    }
                }
            }
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(member.getCode().getPrefLabel().get(language));
            }

            row.createCell(k++).setCellValue(member.getCode().getCodeValue());
            row.createCell(k++).setCellValue(member.getCode().getUri());
            row.createCell(k++).setCellValue(member.getRelatedMember().getCode().getUri());
        }
    }

    private String resolveRelatedMemberIdentifier(final Set<MemberDTO> members,
                                                  final MemberDTO relatedMember) {
        if (relatedMember == null) {
            return "";
        }
        final CodeDTO relatedCode = relatedMember.getCode();
        if (relatedCode == null) {
            throw new YtiCodeListException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), ERR_MSG_USER_406));
        }
        int count = 0;
        for (final MemberDTO member : members) {
            if (member.getCode().getId().equals(relatedCode.getId())) {
                count++;
                if (count > 1) {
                    return relatedMember.getId().toString();
                }
            }
        }
        return relatedCode.getUri();
    }

    private Set<String> resolveMemberPrefLabelLanguages(final Set<MemberDTO> members) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final MemberDTO member : members) {
            final Map<String, String> prefLabel = member.getPrefLabel();
            languages.addAll(prefLabel.keySet());
        }
        return languages;
    }

    public Workbook createExcel(final ExtensionDTO extension,
                                final Set<MemberDTO> members,
                                final String format) {
        final Workbook workbook = createWorkBook(format);
        addMembersSheet(extension, workbook, EXCEL_SHEET_MEMBERS, members);
        return workbook;
    }
}
