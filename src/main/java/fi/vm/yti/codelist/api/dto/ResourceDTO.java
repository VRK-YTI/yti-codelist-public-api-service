package fi.vm.yti.codelist.api.dto;

import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;

import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.ExtensionDTO;
import fi.vm.yti.codelist.common.dto.Views;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonFilter("resource")
@XmlRootElement
@XmlType(propOrder = { "uri", "prefLabel", "type", "container", "localName", "description", "status", "modified", "contentModified", "statusModified", "languages" })
@Schema(name = "Resource", description = "Resource DTO that represents data for one single container or resource for integration use.")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ResourceDTO implements Serializable {

    public static final String TYPE_CODELIST = "codelist";
    public static final String TYPE_CODE = "code";
    public static final String TYPE_EXTENSION = "extension";

    private static final long serialVersionUID = 1L;

    private Map<String, String> prefLabel;
    private Map<String, String> description;
    private String localName;
    private String uri;
    private String status;
    private Date modified;
    private Date contentModified;
    private Date statusModified;
    private Set<String> languages;
    private String type;
    private String container;

    public ResourceDTO() {
    }

    public ResourceDTO(final CodeSchemeDTO codeSchemeDto) {
        this.prefLabel = codeSchemeDto.getPrefLabel();
        this.description = codeSchemeDto.getDescription();
        this.localName = codeSchemeDto.getCodeValue();
        this.uri = codeSchemeDto.getUri();
        this.status = codeSchemeDto.getStatus();
        this.modified = codeSchemeDto.getModified();
        this.contentModified = codeSchemeDto.getContentModified();
        this.statusModified = codeSchemeDto.getStatusModified();
        this.type = TYPE_CODELIST;
        this.languages = new HashSet<>();
        codeSchemeDto.getLanguageCodes().forEach(languageCode -> languages.add(languageCode.getCodeValue()));
    }

    public ResourceDTO(final CodeDTO codeDto) {
        this.prefLabel = codeDto.getPrefLabel();
        this.description = codeDto.getDescription();
        this.localName = codeDto.getCodeValue();
        this.uri = codeDto.getUri();
        this.status = codeDto.getStatus();
        this.modified = codeDto.getModified();
        this.statusModified = codeDto.getStatusModified();
        this.type = TYPE_CODE;
        this.container = codeDto.getCodeScheme().getUri();
    }

    public ResourceDTO(final ExtensionDTO extensionDto) {
        this.prefLabel = extensionDto.getPrefLabel();
        this.localName = extensionDto.getCodeValue();
        this.uri = extensionDto.getUri();
        this.status = extensionDto.getStatus();
        this.modified = extensionDto.getModified();
        this.statusModified = extensionDto.getStatusModified();
        this.type = TYPE_EXTENSION;
        this.container = extensionDto.getParentCodeScheme().getUri();
    }

    @JsonView(Views.Normal.class)
    public String getUri() {
        return uri;
    }

    public void setUri(final String uri) {
        this.uri = uri;
    }

    @JsonView(Views.Normal.class)
    public Map<String, String> getPrefLabel() {
        return prefLabel;
    }

    public void setPrefLabel(final Map<String, String> prefLabel) {
        this.prefLabel = prefLabel;
    }

    @JsonView(Views.Normal.class)
    public Map<String, String> getDescription() {
        return description;
    }

    public void setDescription(final Map<String, String> description) {
        this.description = description;
    }

    @JsonView(Views.Normal.class)
    public String getLocalName() {
        return localName;
    }

    public void setLocalName(final String localName) {
        this.localName = localName;
    }

    @JsonView(Views.Normal.class)
    public String getStatus() {
        return status;
    }

    public void setStatus(final String status) {
        this.status = status;
    }

    @Schema(format = "dateTime")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSX")
    @JsonView(Views.Normal.class)
    public Date getModified() {
        if (modified != null) {
            return new Date(modified.getTime());
        }
        return null;
    }

    public void setModified(final Date modified) {
        if (modified != null) {
            this.modified = new Date(modified.getTime());
        } else {
            this.modified = null;
        }
    }

    @JsonView(Views.Normal.class)
    public Set<String> getLanguages() {
        return this.languages;
    }

    public void setLanguages(final Set<String> languages) {
        this.languages = languages;
    }

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    @Schema(format = "dateTime")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSX")
    @JsonView(Views.Normal.class)
    public Date getContentModified() {
        return contentModified;
    }

    public void setContentModified(final Date contentModified) {
        this.contentModified = contentModified;
    }

    @Schema(format = "dateTime")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSX")
    @JsonView(Views.Normal.class)
    public Date getStatusModified() {
        return statusModified;
    }

    public void setStatusModified(final Date statusModified) {
        this.statusModified = statusModified;
    }

    public String getContainer() {
        return container;
    }

    public void setContainer(final String container) {
        this.container = container;
    }
}