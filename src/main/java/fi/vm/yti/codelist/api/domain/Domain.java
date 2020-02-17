package fi.vm.yti.codelist.api.domain;

import java.util.List;
import java.util.Set;

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

    Set<CodeRegistryDTO> getCodeRegistries(final String codeRegistryCodeValue,
                                           final String codeRegistryPrefLabel,
                                           final Meta meta,
                                           final List<String> organizations);

    CodeSchemeDTO getCodeScheme(final String codeSchemeId);

    CodeSchemeDTO getCodeScheme(final String codeRegistryCodeValue,
                                final String codeSchemeCodeValue);

    Set<CodeSchemeDTO> getCodeSchemes();

    Set<CodeSchemeDTO> getCodeSchemesByCodeRegistryCodeValue(final String codeRegistryCodeValue,
                                                             final List<String> organizationIds,
                                                             final List<String> userOrganizationIds,
                                                             final boolean includeIncomplete,
                                                             final String language);

    Set<CodeSchemeDTO> getCodeSchemes(final String sortMode,
                                      final List<String> organizationIds,
                                      final List<String> userOrganizationIds,
                                      final boolean includeIncomplete,
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
                                      final Meta meta);

    CodeDTO getCode(final String codeId);

    CodeDTO getCode(final String codeRegistryCodeValue,
                    final String codeSchemeCodeValue,
                    final String codeCodeValue);

    Set<CodeDTO> getCodesByCodeRegistryCodeValueAndCodeSchemeCodeValue(final String codeRegistryCodeValue,
                                                                       final String codeSchemeCodeValue);

    Set<CodeDTO> getCodes(final String codeRegistryCodeValue,
                          final String codeSchemeCodeValue,
                          final String codeCodeValue,
                          final String prefLabel,
                          final Integer hierarchyLevel,
                          final String broaderCodeId,
                          final String language,
                          final List<String> statuses,
                          final Meta meta);

    PropertyTypeDTO getPropertyType(final String propertyTypeId);

    Set<PropertyTypeDTO> getPropertyTypes(final String propertyTypePrefLabel,
                                          final String context,
                                          final String language,
                                          final String type,
                                          final Meta meta);

    ValueTypeDTO getValueType(final String valueTypeId);

    Set<ValueTypeDTO> getValueTypes(final String localName,
                                    final Meta meta);

    ExternalReferenceDTO getExternalReference(final String externalReferenceId);

    Set<ExternalReferenceDTO> getExternalReferences(final CodeSchemeDTO codeScheme);

    Set<ExternalReferenceDTO> getExternalReferences(final String externalReferencePrefLabel,
                                                    final CodeSchemeDTO codeScheme,
                                                    final boolean full,
                                                    final Meta meta);

    Set<ExtensionDTO> getExtensions(final CodeSchemeDTO codeScheme);

    Set<ExtensionDTO> getExtensions(final String extensionPrefLabel,
                                    final Meta meta);

    Set<ExtensionDTO> getExtensions(final CodeSchemeDTO codeScheme,
                                    final String extensionPrefLabel,
                                    final Meta meta);

    ExtensionDTO getExtension(final String codeRegistryCodeValue,
                              final String codeSchemeCodeValue,
                              final String extensionCodeValue);

    ExtensionDTO getExtension(final String extensionId);

    Set<MemberDTO> getMembers(final Meta meta);

    Set<MemberDTO> getMembers(final ExtensionDTO code,
                              final Meta meta);

    Set<MemberDTO> getMembers(final CodeDTO code,
                              final Meta meta);

    MemberDTO getMember(final String memberId,
                        final String extensionCodeValue);

    Set<ResourceDTO> getContainers(final List<String> includedContainerUris,
                                   final List<String> excludedContainerUris,
                                   final String language,
                                   final List<String> statuses,
                                   final String searchTerm,
                                   final List<String> includeIncompleteFrom,
                                   final boolean includeIncomplete,
                                   final Meta meta);

    Set<ResourceDTO> getResources(final List<String> codeSchemeUris,
                                  final List<String> includedResourceUris,
                                  final List<String> excludedResourceUris,
                                  final String language,
                                  final List<String> statuses,
                                  final String type,
                                  final String searchTerm,
                                  final List<String> includeIncompleteFrom,
                                  final boolean includeIncomplete,
                                  final Meta meta);
}
