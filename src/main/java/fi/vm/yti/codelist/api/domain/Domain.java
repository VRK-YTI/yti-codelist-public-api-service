package fi.vm.yti.codelist.api.domain;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import fi.vm.yti.codelist.api.dto.ResourceDTO;
import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.CodeRegistryDTO;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ExtensionDTO;
import fi.vm.yti.codelist.common.dto.ExternalReferenceDTO;
import fi.vm.yti.codelist.common.dto.MemberDTO;
import fi.vm.yti.codelist.common.dto.Meta;
import fi.vm.yti.codelist.common.dto.PropertyTypeDTO;
import fi.vm.yti.codelist.common.dto.ValueTypeDTO;

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

    Set<CodeSchemeDTO> getCodeSchemesByCodeRegistryCodeValue(final String codeRegistryCodeValue,
                                                             final List<String> organizationIds,
                                                             final List<String> userOrganizationIds,
                                                             final String language);

    Set<CodeSchemeDTO> getCodeSchemes(final Integer pageSize,
                                      final Integer from,
                                      final String sortMode,
                                      final List<String> organizationIds,
                                      final List<String> userOrganizationIds,
                                      final String codeRegistryCodeValue,
                                      final String codeRegistryPrefLabel,
                                      final String codeSchemeCodeValue,
                                      final String codeSchemeCodePrefLabel,
                                      final String language,
                                      final String searchTerm,
                                      final boolean searchCodes,
                                      final boolean searchExtensions,
                                      final List<String> statuses,
                                      final List<String> infoDomains,
                                      final String extensionPropertyType,
                                      final Date after,
                                      final Meta meta);

    CodeDTO getCode(final String codeId);

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
                          final String language,
                          final List<String> statuses,
                          final Date after,
                          final Meta meta);

    PropertyTypeDTO getPropertyType(final String propertyTypeId);

    Set<PropertyTypeDTO> getPropertyTypes(final Integer pageSize,
                                          final Integer from,
                                          final String propertyTypePrefLabel,
                                          final String context,
                                          final String language,
                                          final String type,
                                          final Date after,
                                          final Meta meta);

    ValueTypeDTO getValueType(final String valueTypeId);

    Set<ValueTypeDTO> getValueTypes(final Integer pageSize,
                                    final Integer from,
                                    final String localName,
                                    final Date after,
                                    final Meta meta);

    ExternalReferenceDTO getExternalReference(final String externalReferenceId);

    Set<ExternalReferenceDTO> getExternalReferences(final CodeSchemeDTO codeScheme);

    Set<ExternalReferenceDTO> getExternalReferences(final Integer pageSize,
                                                    final Integer from,
                                                    final String externalReferencePrefLabel,
                                                    final CodeSchemeDTO codeScheme,
                                                    final Boolean full,
                                                    final Date after,
                                                    final Meta meta);

    Set<ExtensionDTO> getExtensions(final Integer pageSize,
                                    final Integer from,
                                    final String extensionPrefLabel,
                                    final CodeSchemeDTO codeScheme,
                                    final Date after,
                                    final Meta meta);

    ExtensionDTO getExtension(final String codeRegistryCodeValue,
                              final String codeSchemeCodeValue,
                              final String extensionCodeValue);

    ExtensionDTO getExtension(final String extensionId);

    Set<MemberDTO> getMembers(final Integer pageSize,
                              final Integer from,
                              final Date after,
                              final Meta meta);

    Set<MemberDTO> getMembers(final Integer pageSize,
                              final Integer from,
                              final ExtensionDTO code,
                              final Date after,
                              final Meta meta);

    Set<MemberDTO> getMembers(final Integer pageSize,
                              final Integer from,
                              final CodeDTO code,
                              final Date after,
                              final Meta meta);

    MemberDTO getMember(final String memberId,
                        final String extensionCodeValue);

    Set<ResourceDTO> getContainers(final Integer pageSize,
                                   final Integer from,
                                   final String language,
                                   final List<String> statuses,
                                   final Date after,
                                   final Meta meta);

    Set<ResourceDTO> getResources(final Integer pageSize,
                                  final Integer from,
                                  final String codeSchemeUri,
                                  final String language,
                                  final List<String> statuses,
                                  final Date after,
                                  final Meta meta);
}
