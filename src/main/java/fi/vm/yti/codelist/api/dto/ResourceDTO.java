package fi.vm.yti.codelist.api.dto;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;

import fi.vm.yti.codelist.common.dto.CodeDTO;
import fi.vm.yti.codelist.common.dto.CodeSchemeDTO;
import fi.vm.yti.codelist.common.dto.Views;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@JsonFilter("resource")
@XmlRootElement
@XmlType(propOrder = { "uri", "prefLabel", "localName", "description", "status", "modified" })
@ApiModel(value = "Resource", description = "Resource DTO that represents data for one single container or resource for integration use.")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ResourceDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    private Map<String, String> prefLabel;
    private Map<String, String> description;
    private String localName;
    private String uri;
    private String status;
    private Date modified;

    public ResourceDTO() {
    }

    public ResourceDTO(final CodeSchemeDTO codeSchemeDto) {
        this.prefLabel = codeSchemeDto.getPrefLabel();
        this.description = codeSchemeDto.getDescription();
        this.localName = codeSchemeDto.getCodeValue();
        this.uri = codeSchemeDto.getUri();
        this.status = codeSchemeDto.getStatus();
        this.modified = codeSchemeDto.getModified();
    }

    public ResourceDTO(final CodeDTO codeDto) {
        this.prefLabel = codeDto.getPrefLabel();
        this.description = codeDto.getDescription();
        this.localName = codeDto.getCodeValue();
        this.uri = codeDto.getUri();
        this.status = codeDto.getStatus();
        this.modified = codeDto.getModified();
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
    public String getStatus() {
        return status;
    }

    public void setStatus(final String status) {
        this.status = status;
    }

    @ApiModelProperty(dataType = "dateTime")
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
}