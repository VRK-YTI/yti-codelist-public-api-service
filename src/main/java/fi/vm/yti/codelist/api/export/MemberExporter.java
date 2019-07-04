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
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
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
        final Set<CodeDTO> codesInMembers = members.stream().map(MemberDTO::getCode).collect(Collectors.toSet());
        final Set<String> codePrefLabelLanguages = resolveCodePrefLabelLanguages(codesInMembers);
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, csvSeparator, CONTENT_HEADER_MEMBER_ID);
        appendValue(csv, csvSeparator, CONTENT_HEADER_URI);
        final Set<ValueTypeDTO> valueTypes = extension != null ? extension.getPropertyType().getValueTypes() : null;
        if (valueTypes != null && !valueTypes.isEmpty()) {
            valueTypes.forEach(valueType -> appendValue(csv, csvSeparator, valueType.getLocalName().toUpperCase()));
        }
        prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        appendValue(csv, csvSeparator, CONTENT_HEADER_CODE);
        codePrefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_CODE_PREFLABEL_PREFIX + language.toUpperCase()));
        appendValue(csv, csvSeparator, CONTENT_HEADER_RELATION);
        appendValue(csv, csvSeparator, CONTENT_HEADER_STARTDATE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ENDDATE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_CREATED);
        appendValue(csv, csvSeparator, CONTENT_HEADER_MODIFIED);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ORDER, true);
        for (final MemberDTO member : members) {
            appendValue(csv, csvSeparator, member.getSequenceId() != null ? member.getSequenceId().toString() : "");
            appendValue(csv, csvSeparator, member.getUri());
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
            prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, getMemberPrefLabel(member, language)));
            final CodeDTO memberCode = member.getCode();
            appendValue(csv, csvSeparator, resolveMemberCodeIdentifier(extension.getParentCodeScheme(), member.getCode()));
            codePrefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, memberCode != null ? getCodePrefLabel(memberCode, language) : ""));
            appendValue(csv, csvSeparator, resolveRelatedMemberIdentifier(member.getRelatedMember()));
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
        final Set<CodeDTO> codesInMembers = members.stream().map(MemberDTO::getCode).collect(Collectors.toSet());
        final Set<String> prefLabelLanguages = resolveCodePrefLabelLanguages(codesInMembers);
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        final Set<ValueTypeDTO> valueTypes = extension != null ? extension.getPropertyType().getValueTypes() : null;
        if (valueTypes != null && !valueTypes.isEmpty()) {
            valueTypes.forEach(valueType -> appendValue(csv, csvSeparator, valueType.getLocalName().toUpperCase()));
        }
        appendValue(csv, csvSeparator, CONTENT_HEADER_URI1 + "_" + CONTENT_HEADER_CODEVALUE);
        prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_URI1 + "_" + CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
        appendValue(csv, csvSeparator, CONTENT_HEADER_URI1);
        appendValue(csv, csvSeparator, CONTENT_HEADER_URI2 + "_" + CONTENT_HEADER_CODEVALUE);
        prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, CONTENT_HEADER_URI2 + "_" + CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase()));
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
            appendValue(csv, csvSeparator, member.getCode() != null ? member.getCode().getCodeValue() : "");
            prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, getCodePrefLabel(member.getCode(), language)));
            appendValue(csv, csvSeparator, member.getCode() != null ? member.getCode().getUri() : "");

            if (member.getRelatedMember() != null) {
                appendValue(csv, csvSeparator, member.getRelatedMember().getCode() != null ? member.getRelatedMember().getCode().getCodeValue() : "");
                prefLabelLanguages.forEach(language -> appendValue(csv, csvSeparator, getCodePrefLabel(member.getRelatedMember().getCode(), language)));
                appendValue(csv, csvSeparator, member.getRelatedMember().getCode().getUri(), true);
            }
        }
        return csv.toString();
    }

    void addMembersSheet(final ExtensionDTO extension,
                                final Workbook workbook,
                                final String sheetName,
                                final Set<MemberDTO> members) {
        final Set<String> prefLabelLanguages = resolveMemberPrefLabelLanguages(members);
        final Set<CodeDTO> codesInMembers = members.stream().map(MemberDTO::getCode).collect(Collectors.toSet());
        final Set<String> codePrefLabelLanguages = resolveCodePrefLabelLanguages(codesInMembers);
        final Sheet sheet = workbook.createSheet(sheetName);
        final Row rowHead = sheet.createRow((short) 0);
        int j = 0;
        rowHead.createCell(j++).setCellValue(CONTENT_HEADER_MEMBER_ID);
        rowHead.createCell(j++).setCellValue(CONTENT_HEADER_URI);
        final Set<ValueTypeDTO> valueTypes = extension != null ? extension.getPropertyType().getValueTypes() : null;
        appendValueTypeHeaders(valueTypes, rowHead, j);
        if (valueTypes != null && !valueTypes.isEmpty()) {
            for (final ValueTypeDTO valueType : valueTypes) {
                rowHead.createCell(j++).setCellValue(valueType.getLocalName().toUpperCase());
            }
        }
        for (final String language : prefLabelLanguages) {
            rowHead.createCell(j++).setCellValue(CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        }
        rowHead.createCell(j++).setCellValue(CONTENT_HEADER_CODE);
        for (final String language : codePrefLabelLanguages) {
            rowHead.createCell(j++).setCellValue(CONTENT_HEADER_CODE_PREFLABEL_PREFIX + language.toUpperCase());
        }
        rowHead.createCell(j++).setCellValue(CONTENT_HEADER_RELATION);
        rowHead.createCell(j++).setCellValue(CONTENT_HEADER_STARTDATE);
        rowHead.createCell(j++).setCellValue(CONTENT_HEADER_ENDDATE);
        rowHead.createCell(j++).setCellValue(CONTENT_HEADER_CREATED);
        rowHead.createCell(j++).setCellValue(CONTENT_HEADER_MODIFIED);
        rowHead.createCell(j).setCellValue(CONTENT_HEADER_ORDER);
        int i = 1;
        for (final MemberDTO member : members) {
            final Row row = sheet.createRow(i++);
            int k = 0;
            row.createCell(k++).setCellValue(member.getSequenceId() != null ? member.getSequenceId().toString() : "");
            row.createCell(k++).setCellValue(member.getUri());
            appendValueTypes(valueTypes, member, row, k);
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(getMemberPrefLabel(member, language));
            }
            final CodeDTO memberCode = member.getCode();
            row.createCell(k++).setCellValue(resolveMemberCodeIdentifier(extension.getParentCodeScheme(), memberCode));
            for (final String language : codePrefLabelLanguages) {
                row.createCell(k++).setCellValue(memberCode != null ? getCodePrefLabel(memberCode, language) : "");
            }
            row.createCell(k++).setCellValue(resolveRelatedMemberIdentifier(member.getRelatedMember()));
            row.createCell(k++).setCellValue(member.getStartDate() != null ? formatDateWithISO8601(member.getStartDate()) : "");
            row.createCell(k++).setCellValue(member.getEndDate() != null ? formatDateWithISO8601(member.getEndDate()) : "");
            row.createCell(k++).setCellValue(member.getCreated() != null ? formatDateWithSeconds(member.getCreated()) : "");
            row.createCell(k++).setCellValue(member.getModified() != null ? formatDateWithSeconds(member.getModified()) : "");
            row.createCell(k).setCellValue(checkEmptyValue(member.getOrder() != null ? member.getOrder().toString() : ""));
        }
    }

    void addMembersSheetWithCrossRerefences(final ExtensionDTO extension,
                                                   final Workbook workbook,
                                                   final String sheetName,
                                                   final Set<MemberDTO> members) {
        Set<CodeDTO> codesInMembers = members.stream().map(MemberDTO::getCode).collect(Collectors.toSet());
        final Set<String> prefLabelLanguages = resolveCodePrefLabelLanguages(codesInMembers);
        final Sheet sheet = workbook.createSheet(sheetName);
        final Row rowHead = sheet.createRow((short) 0);
        int j = 0;
        final Set<ValueTypeDTO> valueTypes = extension != null ? extension.getPropertyType().getValueTypes() : null;
        appendValueTypeHeaders(valueTypes, rowHead, j);
        rowHead.createCell(j++).setCellValue(CONTENT_HEADER_URI1 + "_" + CONTENT_HEADER_CODEVALUE);
        for (final String language : prefLabelLanguages) {
            rowHead.createCell(j++).setCellValue(CONTENT_HEADER_URI1 + "_" + CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        }
        rowHead.createCell(j++).setCellValue(CONTENT_HEADER_URI1);
        rowHead.createCell(j++).setCellValue(CONTENT_HEADER_URI2 + "_" + CONTENT_HEADER_CODEVALUE);
        for (final String language : prefLabelLanguages) {
            rowHead.createCell(j++).setCellValue(CONTENT_HEADER_URI2 + "_" + CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        }
        rowHead.createCell(j).setCellValue(CONTENT_HEADER_URI2);
        int i = 1;
        for (final MemberDTO member : members) {
            if (member.getRelatedMember() == null) {
                continue;
            }
            final Row row = sheet.createRow(i++);
            int k = 0;
            appendValueTypes(valueTypes, member, row, k);
            row.createCell(k++).setCellValue(member.getCode().getCodeValue());
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(getCodePrefLabel(member.getCode(), language));
            }
            row.createCell(k++).setCellValue(member.getCode().getUri());
            if (member.getRelatedMember() != null) {
                row.createCell(k++).setCellValue(member.getRelatedMember().getCode().getCodeValue());
                for (final String language : prefLabelLanguages) {
                    row.createCell(k++).setCellValue(getCodePrefLabel(member.getRelatedMember().getCode(), language));
                }
                row.createCell(k).setCellValue(member.getRelatedMember().getCode().getUri());
            }
        }
    }

    private void appendValueTypeHeaders(final Set<ValueTypeDTO> valueTypes,
                                        final Row rowHead,
                                        int j) {
        if (valueTypes != null && !valueTypes.isEmpty()) {
            for (final ValueTypeDTO valueType : valueTypes) {
                rowHead.createCell(j++).setCellValue(valueType.getLocalName().toUpperCase());
            }
        }
    }

    private void appendValueTypes(final Set<ValueTypeDTO> valueTypes,
                                  final MemberDTO member,
                                  final Row row,
                                  int k) {
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
    }

    private String resolveMemberCodeIdentifier(final CodeSchemeDTO codeScheme,
                                               final CodeDTO code) {
        if (codeScheme != null && code != null && codeScheme.getId().equals(code.getCodeScheme().getId())) {
            return code.getCodeValue();
        } else if (code != null) {
            return code.getUri();
        }
        return "";
    }

    private String resolveRelatedMemberIdentifier(final MemberDTO relatedMember) {
        if (relatedMember == null) {
            return "";
        }
        final CodeDTO relatedCode = relatedMember.getCode();
        if (relatedCode == null) {
            throw new YtiCodeListException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), ERR_MSG_USER_406));
        }
        return relatedMember.getSequenceId().toString();
    }

    private Set<String> resolveMemberPrefLabelLanguages(final Set<MemberDTO> members) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final MemberDTO member : members) {
            final Map<String, String> prefLabel = member.getPrefLabel();
            if (prefLabel != null && !prefLabel.isEmpty()) {
                languages.addAll(prefLabel.keySet());
            }
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
