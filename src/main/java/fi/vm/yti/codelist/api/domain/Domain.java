package fi.vm.yti.codelist.api.domain;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.CodeRegistryDTO;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ExtensionDTO;
import fi.vm.yti.codelist.common.dto.ExtensionSchemeDTO;
import fi.vm.yti.codelist.common.dto.ExternalReferenceDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import fi.vm.yti.codelist.common.dto.PropertyTypeDTO;

public interface Domain {

    CodeRegistryDTO getCodeRegistry(final String codeRegistryCodeValue);

    Set<CodeRegistryDTO> getCodeRegistries();

    Set<CodeRegistryDTO> getCodeRegistries(final Integer pageSize,
                                           final Integer from,
                                           final String codeRegistryCodeValue,
                                           final String codeRegistryPrefLabel,
                                           final Date after,
                                           final Meta meta,
                                           final List<String> organizations);

    CodeSchemeDTO getCodeScheme(final String codeSchemeId);

    CodeSchemeDTO getCodeScheme(final String codeRegistryCodeValue,
                                final String codeSchemeCodeValue);

    Set<CodeSchemeDTO> getCodeSchemes(final String language);

    Set<CodeSchemeDTO> getCodeSchemesByCodeRegistryCodeValue(final String codeRegistryCodeValue, final String language);

    Set<CodeSchemeDTO> getCodeSchemes(final Integer pageSize,
                                      final Integer from,
                                      final String sortMode,
                                      final String organizationId,
                                      final String codeRegistryCodeValue,
                                      final String codeRegistryPrefLabel,
                                      final String codeSchemeCodeValue,
                                      final String codeSchemeCodePrefLabel,
                                      final String language,
                                      final String searchTerm,
                                      final boolean searchCodes,
                                      final List<String> statuses,
                                      final List<String> dataClassifications,
                                      final Date after,
                                      final Meta meta);

    CodeDTO getCode(final String codeRegistryCodeValue,
                    final String codeSchemeCodeValue,
                    final String codeCodeValue);

    Set<CodeDTO> getCodesByCodeRegistryCodeValueAndCodeSchemeCodeValue(final String codeRegistryCodeValue,
                                                                       final String codeSchemeCodeValue);

    Set<CodeDTO> getCodes(final Integer pageSize,
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

    PropertyTypeDTO getPropertyType(final String propertyTypeId);

    Set<PropertyTypeDTO> getPropertyTypes();

    Set<PropertyTypeDTO> getPropertyTypes(final Integer pageSize,
                                          final Integer from,
                                          final String propertyTypePrefLabel,
                                          final String context,
                                          final String type,
                                          final Date after,
                                          final Meta meta);

    ExternalReferenceDTO getExternalReference(final String externalReferenceId);

    Set<ExternalReferenceDTO> getExternalReferences();

    Set<ExternalReferenceDTO> getExternalReferences(final Integer pageSize,
                                                    final Integer from,
                                                    final String externalReferencePrefLabel,
                                                    final CodeSchemeDTO codeScheme,
                                                    final Date after,
                                                    final Meta meta);

    Set<ExtensionSchemeDTO> getExtensionSchemes(final Integer pageSize,
                                                final Integer from,
                                                final String extensionSchemePrefLabel,
                                                final CodeSchemeDTO codeScheme,
                                                final Date after,
                                                final Meta meta);

    ExtensionSchemeDTO getExtensionScheme(final String codeRegistryCodeValue,
                                          final String codeSchemeCodeValue,
                                          final String extensionSchemeCodeValue);

    ExtensionSchemeDTO getExtensionScheme(final UUID codeSchemeId,
                                          final String extensionSchemeCodeValue);

    ExtensionSchemeDTO getExtensionScheme(final String extensionSchemeId);

    Set<ExtensionDTO> getExtensions(final Integer pageSize,
                                    final Integer from,
                                    final Date after,
                                    final Meta meta);

    Set<ExtensionDTO> getExtensions(final Integer pageSize,
                                    final Integer from,
                                    final ExtensionSchemeDTO code,
                                    final Date after,
                                    final Meta meta);

    Set<ExtensionDTO> getExtensions(final Integer pageSize,
                                    final Integer from,
                                    final CodeDTO code,
                                    final Date after,
                                    final Meta meta);

    ExtensionDTO getExtension(final String extensionId);
}
