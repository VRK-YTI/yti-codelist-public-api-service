package fi.vm.yti.codelist.api.dto;

import java.util.List;

public class IntegrationResourceRequestDTO {

    private List<String> container;
    private Integer pageFrom;
    private Integer pageSize;
    private List<String> status;
    private String after;
    private String before;
    private List<String> filter;
    private String language;
    private String searchTerm;
    private String pretty;
    private List<String> includeIncompleteFrom;
    private List<String> uri;
    private String type;
    private boolean includeIncomplete;

    public List<String> getContainer() {
        return container;
    }

    public void setContainer(final List<String> container) {
        this.container = container;
    }

    public Integer getPageFrom() {
        return pageFrom;
    }

    public void setPageFrom(final Integer pageFrom) {
        this.pageFrom = pageFrom;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(final Integer pageSize) {
        this.pageSize = pageSize;
    }

    public List<String> getStatus() {
        return status;
    }

    public void setStatus(final List<String> status) {
        this.status = status;
    }

    public String getAfter() {
        return after;
    }

    public void setAfter(final String after) {
        this.after = after;
    }

    public List<String> getFilter() {
        return filter;
    }

    public void setFilter(final List<String> filter) {
        this.filter = filter;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(final String language) {
        this.language = language;
    }

    public String getSearchTerm() {
        return searchTerm;
    }

    public void setSearchTerm(final String searchTerm) {
        this.searchTerm = searchTerm;
    }

    public String getPretty() {
        return pretty;
    }

    public void setPretty(final String pretty) {
        this.pretty = pretty;
    }

    public List<String> getIncludeIncompleteFrom() {
        return includeIncompleteFrom;
    }

    public void setIncludeIncompleteFrom(final List<String> includeIncompleteFrom) {
        this.includeIncompleteFrom = includeIncompleteFrom;
    }

    public boolean getIncludeIncomplete() {
        return includeIncomplete;
    }

    public void setIncludeIncomplete(final boolean includeIncomplete) {
        this.includeIncomplete = includeIncomplete;
    }

    public List<String> getUri() {
        return uri;
    }

    public void setUri(final List<String> uri) {
        this.uri = uri;
    }

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    public String getBefore() {
        return before;
    }

    public void setBefore(final String before) {
        this.before = before;
    }
}
