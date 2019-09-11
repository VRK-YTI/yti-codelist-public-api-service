package fi.vm.yti.codelist.api.dto;

import java.util.List;

public class IntegrationResourceRequestDTO {

    private String container;
    private Integer from;
    private Integer pageSize;
    private List<String> status;
    private String after;
    private List<String> filter;
    private String language;
    private String searchTerm;
    private String pretty;

    public String getContainer() {
        return container;
    }

    public void setContainer(final String container) {
        this.container = container;
    }

    public Integer getFrom() {
        return from;
    }

    public void setFrom(final Integer from) {
        this.from = from;
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
}
