package fi.vm.yti.codelist.api.domain;

import java.util.Date;
import java.util.List;
import java.util.Set;

import fi.vm.yti.codelist.common.model.Code;
import fi.vm.yti.codelist.common.model.CodeRegistry;
import fi.vm.yti.codelist.common.model.CodeScheme;
import fi.vm.yti.codelist.common.model.ExternalReference;
import fi.vm.yti.codelist.common.model.Meta;
import fi.vm.yti.codelist.common.model.PropertyType;

public interface Domain {

    CodeRegistry getCodeRegistry(final String codeRegistryCodeValue);

    Set<CodeRegistry> getCodeRegistries();

    Set<CodeRegistry> getCodeRegistries(final Integer pageSize,
                                        final Integer from,
                                        final String codeRegistryCodeValue,
                                        final String codeRegistryPrefLabel,
                                        final Date after,
                                        final Meta meta,
                                        final List<String> organizations);

    CodeScheme getCodeSchemeWithId(final String codeSchemeId);

    CodeScheme getCodeScheme(final String codeSchemeId);

    CodeScheme getCodeScheme(final String codeRegistryCodeValue,
                             final String codeSchemeCodeValue);

    Set<CodeScheme> getCodeSchemes();

    Set<CodeScheme> getCodeSchemes(final Integer pageSize,
                                   final Integer from,
                                   final String organizationId,
                                   final String codeRegistryCodeValue,
                                   final String codeRegistryPrefLabel,
                                   final String codeSchemeCodeValue,
                                   final String codeSchemeCodePrefLabel,
                                   final List<String> statuses,
                                   final List<String> dataClassifications,
                                   final Date after,
                                   final Meta meta);

    Code getCode(final String codeRegistryCodeValue,
                 final String codeSchemeCodeValue,
                 final String codeCodeValue);

    Set<Code> getCodes(final Integer pageSize,
                       final Integer from,
                       final String codeRegistryCodeValue,
                       final String codeSchemeCodeValue,
                       final String codeCodeValue,
                       final String prefLabel,
                       final Integer hierarchyLevel,
                       final String broaderCodeId,
                       final List<String> statuses,
                       final Date after,
                       final Meta meta);

    PropertyType getPropertyType(final String propertyTypeId);

    Set<PropertyType> getPropertyTypes();

    Set<PropertyType> getPropertyTypes(final Integer pageSize,
                                       final Integer from,
                                       final String propertyTypePrefLabel,
                                       final String context,
                                       final Date after,
                                       final Meta meta);

    ExternalReference getExternalReference(final String externalReferenceId);

    Set<ExternalReference> getExternalReferences();

    Set<ExternalReference> getExternalReferences(final Integer pageSize,
                                                 final Integer from,
                                                 final String externalReferencePrefLabel,
                                                 final CodeScheme codeScheme,
                                                 final Date after,
                                                 final Meta meta);
}
