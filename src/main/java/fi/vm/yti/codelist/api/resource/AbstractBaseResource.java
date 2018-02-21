package fi.vm.yti.codelist.api.resource;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.jaxrs.cfg.EndpointConfigBase;
import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterModifier;

import fi.vm.yti.codelist.api.api.ErrorWrapper;
import fi.vm.yti.codelist.common.model.Code;
import fi.vm.yti.codelist.common.model.CodeRegistry;
import fi.vm.yti.codelist.common.model.CodeScheme;
import fi.vm.yti.codelist.common.model.Meta;
import fi.vm.yti.codelist.common.model.Status;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_BROADER;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_CHANGENOTE_PREFIX;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_CLASSIFICATION;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_CODEVALUE;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_DEFINITION_PREFIX;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_DESCRIPTION_PREFIX;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_ENDDATE;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_GOVERNANCEPOLICY;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_HIERARCHYLEVEL;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_ID;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_LEGALBASE;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_PREFLABEL_PREFIX;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_SHORTNAME;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_SOURCE;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_STARTDATE;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_STATUS;
import static fi.vm.yti.codelist.common.constants.ApiConstants.CONTENT_HEADER_VERSION;
import static fi.vm.yti.codelist.common.constants.ApiConstants.EXCEL_SHEET_CODEREGISTRIES;
import static fi.vm.yti.codelist.common.constants.ApiConstants.EXCEL_SHEET_CODES;
import static fi.vm.yti.codelist.common.constants.ApiConstants.EXCEL_SHEET_CODESCHEMES;
import static fi.vm.yti.codelist.common.constants.ApiConstants.FIELD_NAME_ID;
import static fi.vm.yti.codelist.common.constants.ApiConstants.FIELD_NAME_URI;
import static fi.vm.yti.codelist.common.constants.ApiConstants.FILTER_NAME_CODE;
import static fi.vm.yti.codelist.common.constants.ApiConstants.FILTER_NAME_CODEREGISTRY;
import static fi.vm.yti.codelist.common.constants.ApiConstants.FILTER_NAME_CODESCHEME;
import static fi.vm.yti.codelist.common.constants.ApiConstants.FILTER_NAME_DATACLASSIFICATION;
import static fi.vm.yti.codelist.common.constants.ApiConstants.FILTER_NAME_EXTERNALREFERENCE;
import static fi.vm.yti.codelist.common.constants.ApiConstants.FILTER_NAME_ORGANIZATION;
import static fi.vm.yti.codelist.common.constants.ApiConstants.FILTER_NAME_PROPERTYTYPE;
import static fi.vm.yti.codelist.common.constants.ApiConstants.FORMAT_CSV;
import static fi.vm.yti.codelist.common.constants.ApiConstants.FORMAT_EXCEL;
import static fi.vm.yti.codelist.common.constants.ApiConstants.FORMAT_EXCEL_XLS;
import static fi.vm.yti.codelist.common.constants.ApiConstants.FORMAT_EXCEL_XLSX;

abstract class AbstractBaseResource {

    public static final String DOWNLOAD_FILENAME_CODEREGISTRIES = "coderegistries";
    public static final String DOWNLOAD_FILENAME_CODESCHEMES = "codeschemes";
    public static final String DOWNLOAD_FILENAME_CODES = "codes";

    public static final String HEADER_CONTENT_DISPOSITION = "content-disposition";

    public SimpleFilterProvider createSimpleFilterProvider(final String baseFilter,
                                                           final String expand) {
        final List<String> baseFilters = new ArrayList<>();
        baseFilters.add(baseFilter);
        return createSimpleFilterProvider(baseFilters, expand);
    }

    public SimpleFilterProvider createSimpleFilterProvider(final List<String> baseFilters,
                                                           final String expand) {
        final SimpleFilterProvider filterProvider = new SimpleFilterProvider();
        filterProvider.addFilter(FILTER_NAME_CODEREGISTRY, SimpleBeanPropertyFilter.filterOutAllExcept(FIELD_NAME_URI));
        filterProvider.addFilter(FILTER_NAME_CODESCHEME, SimpleBeanPropertyFilter.filterOutAllExcept(FIELD_NAME_URI));
        filterProvider.addFilter(FILTER_NAME_CODE, SimpleBeanPropertyFilter.filterOutAllExcept(FIELD_NAME_URI));
        filterProvider.addFilter(FILTER_NAME_EXTERNALREFERENCE, SimpleBeanPropertyFilter.filterOutAllExcept(FIELD_NAME_URI));
        filterProvider.addFilter(FILTER_NAME_PROPERTYTYPE, SimpleBeanPropertyFilter.filterOutAllExcept(FIELD_NAME_URI));
        filterProvider.addFilter(FILTER_NAME_DATACLASSIFICATION, SimpleBeanPropertyFilter.filterOutAllExcept(FIELD_NAME_URI));
        filterProvider.addFilter(FILTER_NAME_ORGANIZATION, SimpleBeanPropertyFilter.filterOutAllExcept(FIELD_NAME_ID));
        filterProvider.setFailOnUnknownId(false);
        for (final String baseFilter : baseFilters) {
            filterProvider.removeFilter(baseFilter);
        }
        if (expand != null && !expand.isEmpty()) {
            final List<String> filterOptions = Arrays.asList(expand.split(","));
            for (final String filter : filterOptions) {
                filterProvider.removeFilter(filter);
            }
        }
        return filterProvider;
    }

    Response createErrorResponse(final int errorCode,
                                 final String errorMessage) {
        final ErrorWrapper error = new ErrorWrapper();
        final Meta meta = new Meta();
        meta.setCode(errorCode);
        meta.setMessage(errorMessage);
        error.setMeta(meta);
        return Response.status(Response.Status.NOT_FOUND).entity(error).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

    public List<String> parseStatus(final String statusCsl) {
        final Set<String> statusSet = new HashSet<>();
        if (statusCsl != null) {
            for (final String s : Arrays.asList(statusCsl.split(","))) {
                final Status status = Status.valueOf(s.toUpperCase().trim());
                statusSet.add(status.toString());
            }
        }
        return new ArrayList<String>(statusSet);
    }

    public List<String> parseDataClassifications(final String dataClassificationCsl) {
        final Set<String> dataClassificationsSet = new HashSet<>();
        if (dataClassificationCsl != null) {
            for (final String s : Arrays.asList(dataClassificationCsl.split(","))) {
                dataClassificationsSet.add(s.toUpperCase().trim());
            }
        }
        return new ArrayList<String>(dataClassificationsSet);
    }

    public void logApiRequest(final Logger logger,
                              final String method,
                              final String apiVersionPath,
                              final String apiPath) {
        logger.info(method + " " + apiVersionPath + apiPath + " requested!");
    }

    public void appendNotNull(final StringBuilder builder,
                              final String string) {
        if (string != null) {
            builder.append(string);
        }
    }

    private Set<String> resolveCodeRegistryPrefLabelLanguages(final Set<CodeRegistry> registries) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeRegistry registry : registries) {
            final Map<String, String> prefLabel = registry.getPrefLabel();
            languages.addAll(prefLabel.keySet());
        }
        return languages;
    }

    private Set<String> resolveCodeRegistryDefinitionLanguages(final Set<CodeRegistry> registries) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeRegistry registry : registries) {
            final Map<String, String> definition = registry.getDefinition();
            languages.addAll(definition.keySet());
        }
        return languages;
    }

    public String constructRegistersCsv(final Set<CodeRegistry> registries) {
        final Set<String> prefLabelLanguages = resolveCodeRegistryPrefLabelLanguages(registries);
        final Set<String> definitionLanguages = resolveCodeRegistryDefinitionLanguages(registries);
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, csvSeparator, CONTENT_HEADER_CODEVALUE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ID);
        prefLabelLanguages.forEach(language -> {
            appendValue(csv, csvSeparator, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        });
        definitionLanguages.forEach(language -> {
            appendValue(csv, csvSeparator, CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase());
        });
        csv.append("\n");
        for (final CodeRegistry codeRegistry : registries) {
            appendValue(csv, csvSeparator, codeRegistry.getCodeValue());
            appendValue(csv, csvSeparator, codeRegistry.getId().toString());
            prefLabelLanguages.forEach(language -> {
                appendValue(csv, csvSeparator, codeRegistry.getPrefLabel().get(language));
            });
            definitionLanguages.forEach(language -> {
                appendValue(csv, csvSeparator, codeRegistry.getDefinition().get(language));
            });
            csv.append("\n");
        }
        return csv.toString();
    }

    public Workbook constructRegistriesExcel(final String format,
                                             final Set<CodeRegistry> registries) {
        final Workbook workbook = createWorkBook(format);
        final Set<String> prefLabelLanguages = resolveCodeRegistryPrefLabelLanguages(registries);
        final Set<String> definitionLanguages = resolveCodeRegistryDefinitionLanguages(registries);
        final Sheet sheet = workbook.createSheet(EXCEL_SHEET_CODEREGISTRIES);
        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODEVALUE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ID);
        for (final String language : prefLabelLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        }
        for (final String language : definitionLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase());
        }
        int i = 1;
        for (final CodeRegistry codeRegistry : registries) {
            final Row row = sheet.createRow(i++);
            int k = 0;
            row.createCell(k++).setCellValue(checkEmptyValue(codeRegistry.getCodeValue()));
            row.createCell(k++).setCellValue(checkEmptyValue(codeRegistry.getId().toString()));
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(codeRegistry.getPrefLabel().get(language));
            }
            for (final String language : definitionLanguages) {
                row.createCell(k++).setCellValue(codeRegistry.getDefinition().get(language));
            }
        }
        return workbook;
    }

    private Set<String> resolveCodeSchemePrefLabelLanguages(final Set<CodeScheme> codeSchemes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeScheme codeScheme : codeSchemes) {
            final Map<String, String> prefLabel = codeScheme.getPrefLabel();
            languages.addAll(prefLabel.keySet());
        }
        return languages;
    }

    private Set<String> resolveCodeSchemeDefinitionLanguages(final Set<CodeScheme> codeSchemes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeScheme codeScheme : codeSchemes) {
            final Map<String, String> definition = codeScheme.getDefinition();
            languages.addAll(definition.keySet());
        }
        return languages;
    }

    private Set<String> resolveCodeSchemeDescriptionLanguages(final Set<CodeScheme> codeSchemes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeScheme codeScheme : codeSchemes) {
            final Map<String, String> description = codeScheme.getDescription();
            languages.addAll(description.keySet());
        }
        return languages;
    }

    private Set<String> resolveCodeSchemeChangeNoteLanguages(final Set<CodeScheme> codeSchemes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final CodeScheme codeScheme : codeSchemes) {
            final Map<String, String> changeNote = codeScheme.getChangeNote();
            languages.addAll(changeNote.keySet());
        }
        return languages;
    }

    public String constructCodeSchemesCsv(final Set<CodeScheme> codeSchemes) {
        final Set<String> prefLabelLanguages = resolveCodeSchemePrefLabelLanguages(codeSchemes);
        final Set<String> definitionLanguages = resolveCodeSchemeDefinitionLanguages(codeSchemes);
        final Set<String> descriptionLanguages = resolveCodeSchemeDescriptionLanguages(codeSchemes);
        final Set<String> changeNoteLanguages = resolveCodeSchemeChangeNoteLanguages(codeSchemes);
        final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
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
        prefLabelLanguages.forEach(language -> {
            appendValue(csv, csvSeparator, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        });
        definitionLanguages.forEach(language -> {
            appendValue(csv, csvSeparator, CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase());
        });
        descriptionLanguages.forEach(language -> {
            appendValue(csv, csvSeparator, CONTENT_HEADER_DESCRIPTION_PREFIX + language.toUpperCase());
        });
        changeNoteLanguages.forEach(language -> {
            appendValue(csv, csvSeparator, CONTENT_HEADER_CHANGENOTE_PREFIX + language.toUpperCase());
        });
        appendValue(csv, csvSeparator, CONTENT_HEADER_STARTDATE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ENDDATE, true);
        for (final CodeScheme codeScheme : codeSchemes) {
            appendValue(csv, csvSeparator, codeScheme.getCodeValue());
            appendValue(csv, csvSeparator, codeScheme.getId().toString());
            appendValue(csv, csvSeparator, formatDataClassificationsToString(codeScheme.getDataClassifications()));
            appendValue(csv, csvSeparator, codeScheme.getVersion());
            appendValue(csv, csvSeparator, codeScheme.getStatus());
            appendValue(csv, csvSeparator, codeScheme.getSource());
            appendValue(csv, csvSeparator, codeScheme.getLegalBase());
            appendValue(csv, csvSeparator, codeScheme.getGovernancePolicy());
            prefLabelLanguages.forEach(language -> {
                appendValue(csv, csvSeparator, codeScheme.getPrefLabel().get(language));
            });
            definitionLanguages.forEach(language -> {
                appendValue(csv, csvSeparator, codeScheme.getDefinition().get(language));
            });
            descriptionLanguages.forEach(language -> {
                appendValue(csv, csvSeparator, codeScheme.getDescription().get(language));
            });
            changeNoteLanguages.forEach(language -> {
                appendValue(csv, csvSeparator, codeScheme.getChangeNote().get(language));
            });
            appendValue(csv, csvSeparator, codeScheme.getStartDate() != null ? dateFormat.format(codeScheme.getStartDate()) : "");
            appendValue(csv, csvSeparator, codeScheme.getEndDate() != null ? dateFormat.format(codeScheme.getEndDate()) : "", true);
        }
        return csv.toString();
    }

    public Workbook createWorkBook(final String format) {
        if (FORMAT_EXCEL_XLSX.equals(format)) {
            return new XSSFWorkbook();
        } else {
            return new HSSFWorkbook();
        }
    }

    public Workbook constructCodeSchemesExcel(final String format,
                                              final Set<CodeScheme> codeSchemes) {
        final Workbook workbook = createWorkBook(format);
        final Set<String> prefLabelLanguages = resolveCodeSchemePrefLabelLanguages(codeSchemes);
        final Set<String> definitionLanguages = resolveCodeSchemeDefinitionLanguages(codeSchemes);
        final Set<String> descriptionLanguages = resolveCodeSchemeDescriptionLanguages(codeSchemes);
        final Set<String> changeNoteLanguages = resolveCodeSchemeChangeNoteLanguages(codeSchemes);
        final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        final Sheet sheet = workbook.createSheet(EXCEL_SHEET_CODESCHEMES);
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
        rowhead.createCell(j).setCellValue(CONTENT_HEADER_ENDDATE);
        int i = 1;
        for (final CodeScheme codeScheme : codeSchemes) {
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
            row.createCell(k).setCellValue(codeScheme.getEndDate() != null ? dateFormat.format(codeScheme.getEndDate()) : "");
        }
        return workbook;
    }

    private Set<String> resolveCodePrefLabelLanguages(final Set<Code> codes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final Code code : codes) {
            final Map<String, String> prefLabel = code.getPrefLabel();
            languages.addAll(prefLabel.keySet());
        }
        return languages;
    }

    private Set<String> resolveCodeDefinitionLanguages(final Set<Code> codes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final Code code : codes) {
            final Map<String, String> definition = code.getDefinition();
            languages.addAll(definition.keySet());
        }
        return languages;
    }

    private Set<String> resolveCodeDescriptionLanguages(final Set<Code> codes) {
        final Set<String> languages = new LinkedHashSet<>();
        for (final Code code : codes) {
            final Map<String, String> description = code.getDescription();
            languages.addAll(description.keySet());
        }
        return languages;
    }

    public String constructCodesCsv(final Set<Code> codes) {
        final Map<UUID, String> codeValueIdMap = new HashMap<>();
        for (final Code code : codes) {
            codeValueIdMap.put(code.getId(), code.getCodeValue());
        }
        final Set<String> prefLabelLanguages = resolveCodePrefLabelLanguages(codes);
        final Set<String> definitionLanguages = resolveCodeDefinitionLanguages(codes);
        final Set<String> descriptionLanguages = resolveCodeDescriptionLanguages(codes);
        final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        final String csvSeparator = ",";
        final StringBuilder csv = new StringBuilder();
        appendValue(csv, csvSeparator, CONTENT_HEADER_CODEVALUE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_BROADER);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ID);
        appendValue(csv, csvSeparator, CONTENT_HEADER_STATUS);
        prefLabelLanguages.forEach(language -> {
            appendValue(csv, csvSeparator, CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        });
        definitionLanguages.forEach(language -> {
            appendValue(csv, csvSeparator, CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase());
        });
        descriptionLanguages.forEach(language -> {
            appendValue(csv, csvSeparator, CONTENT_HEADER_DESCRIPTION_PREFIX + language.toUpperCase());
        });
        appendValue(csv, csvSeparator, CONTENT_HEADER_SHORTNAME);
        appendValue(csv, csvSeparator, CONTENT_HEADER_HIERARCHYLEVEL);
        appendValue(csv, csvSeparator, CONTENT_HEADER_STARTDATE);
        appendValue(csv, csvSeparator, CONTENT_HEADER_ENDDATE, true);
        for (final Code code : codes) {
            appendValue(csv, csvSeparator, code.getCodeValue());
            appendValue(csv, csvSeparator, codeValueIdMap.get(code.getBroaderCodeId()));
            appendValue(csv, csvSeparator, code.getId().toString());
            appendValue(csv, csvSeparator, code.getStatus());
            prefLabelLanguages.forEach(language -> {
                appendValue(csv, csvSeparator, code.getPrefLabel().get(language));
            });
            definitionLanguages.forEach(language -> {
                appendValue(csv, csvSeparator, code.getDefinition().get(language));
            });
            descriptionLanguages.forEach(language -> {
                appendValue(csv, csvSeparator, code.getDescription().get(language));
            });
            appendValue(csv, csvSeparator, code.getShortName());
            appendValue(csv, csvSeparator, code.getHierarchyLevel() != null ? code.getHierarchyLevel().toString() : null);
            appendValue(csv, csvSeparator, code.getStartDate() != null ? dateFormat.format(code.getStartDate()) : "");
            appendValue(csv, csvSeparator, code.getEndDate() != null ? dateFormat.format(code.getEndDate()) : "", true);
        }
        return csv.toString();
    }

    public Workbook constructCodesExcel(final String format,
                                        final Set<Code> codes) {
        final Map<UUID, String> codeValueIdMap = new HashMap<>();
        for (final Code code : codes) {
            codeValueIdMap.put(code.getId(), code.getCodeValue());
        }
        final Workbook workbook = createWorkBook(format);
        final Set<String> prefLabelLanguages = resolveCodePrefLabelLanguages(codes);
        final Set<String> definitionLanguages = resolveCodeDefinitionLanguages(codes);
        final Set<String> descriptionLanguages = resolveCodeDescriptionLanguages(codes);
        final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        final Sheet sheet = workbook.createSheet(EXCEL_SHEET_CODES);
        final Row rowhead = sheet.createRow((short) 0);
        int j = 0;
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_CODEVALUE);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_BROADER);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_ID);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_STATUS);
        for (final String language : prefLabelLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_PREFLABEL_PREFIX + language.toUpperCase());
        }
        for (final String language : definitionLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_DEFINITION_PREFIX + language.toUpperCase());
        }
        for (final String language : descriptionLanguages) {
            rowhead.createCell(j++).setCellValue(CONTENT_HEADER_DESCRIPTION_PREFIX + language.toUpperCase());
        }
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_SHORTNAME);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_HIERARCHYLEVEL);
        rowhead.createCell(j++).setCellValue(CONTENT_HEADER_STARTDATE);
        rowhead.createCell(j).setCellValue(CONTENT_HEADER_ENDDATE);
        int i = 1;
        for (final Code code : codes) {
            final Row row = sheet.createRow(i++);
            int k = 0;
            row.createCell(k++).setCellValue(code.getCodeValue());
            row.createCell(k++).setCellValue(checkEmptyValue(codeValueIdMap.get(code.getBroaderCodeId())));
            row.createCell(k++).setCellValue(code.getId().toString());
            row.createCell(k++).setCellValue(code.getStatus());
            for (final String language : prefLabelLanguages) {
                row.createCell(k++).setCellValue(code.getPrefLabel().get(language));
            }
            for (final String language : definitionLanguages) {
                row.createCell(k++).setCellValue(code.getDefinition().get(language));
            }
            for (final String language : descriptionLanguages) {
                row.createCell(k++).setCellValue(code.getDescription().get(language));
            }
            row.createCell(k++).setCellValue(checkEmptyValue(code.getShortName()));
            row.createCell(k++).setCellValue(checkEmptyValue(code.getHierarchyLevel() != null ? code.getHierarchyLevel().toString() : null));
            row.createCell(k++).setCellValue(code.getStartDate() != null ? dateFormat.format(code.getStartDate()) : "");
            row.createCell(k).setCellValue(code.getEndDate() != null ? dateFormat.format(code.getEndDate()) : "");
        }
        return workbook;
    }

    public String checkEmptyValue(final String value) {
        if (value == null) {
            return "";
        }
        return value;
    }

    public void appendValue(final StringBuilder builder, final String separator, final String value) {
        appendValue(builder, separator, value, false);
    }

    private void appendValue(final StringBuilder builder, final String separator, final String value, final boolean isLast) {
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

    public String createDownloadFilename(final String format,
                                         final String filename) {
        if (FORMAT_EXCEL.equalsIgnoreCase(format) || FORMAT_EXCEL_XLS.equalsIgnoreCase(format)) {
            return filename + "." + FORMAT_EXCEL_XLS;
        } else if (FORMAT_EXCEL_XLSX.equalsIgnoreCase(format)) {
            return filename + "." + FORMAT_EXCEL_XLSX;
        } else {
            return filename + "." + FORMAT_CSV;
        }
    }

    private String formatDataClassificationsToString(final Set<Code> classifications) {
        final StringBuilder csvClassifications = new StringBuilder();
        int i = 0;
        for (final Code code : classifications) {
            i++;
            csvClassifications.append(code.getCodeValue().trim());
            if (i < classifications.size()) {
                csvClassifications.append(";");
            }
            i++;
        }
        return csvClassifications.toString();
    }

    static class FilterModifier extends ObjectWriterModifier {

        private final FilterProvider provider;

        protected FilterModifier(final FilterProvider provider) {
            this.provider = provider;
        }

        @Override
        public ObjectWriter modify(final EndpointConfigBase<?> endpoint,
                                   final MultivaluedMap<String, Object> responseHeaders,
                                   final Object valueToWrite,
                                   final ObjectWriter w,
                                   final JsonGenerator g) throws IOException {
            return w.with(provider);
        }
    }
}
