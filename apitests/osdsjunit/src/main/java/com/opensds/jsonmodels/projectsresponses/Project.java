package com.opensds.jsonmodels.projectsresponses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class Project {

    @SerializedName("is_domain")
    @Expose
    private Boolean isDomain;
    @SerializedName("description")
    @Expose
    private String description;
    @SerializedName("links")
    @Expose
    private Links_ links;
    @SerializedName("tags")
    @Expose
    private List<Object> tags = null;
    @SerializedName("enabled")
    @Expose
    private Boolean enabled;
    @SerializedName("id")
    @Expose
    private String id;
    @SerializedName("parent_id")
    @Expose
    private String parentId;
    @SerializedName("domain_id")
    @Expose
    private String domainId;
    @SerializedName("name")
    @Expose
    private String name;

    public Boolean getIsDomain() {
        return isDomain;
    }

    public void setIsDomain(Boolean isDomain) {
        this.isDomain = isDomain;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Links_ getLinks() {
        return links;
    }

    public void setLinks(Links_ links) {
        this.links = links;
    }

    public List<Object> getTags() {
        return tags;
    }

    public void setTags(List<Object> tags) {
        this.tags = tags;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public String getDomainId() {
        return domainId;
    }

    public void setDomainId(String domainId) {
        this.domainId = domainId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("\n\tisDomain=").append(isDomain).
                append("\n\tdescription=").append(description).
                append("\n\tlinks=").append(links)
                .append("\n\ttags=").append(tags).
                        append("\n\tenabled=").append(enabled).
                        append("\n\tid=").append(id).
                        append("\n\tparentId=").append(parentId).
                        append("\n\tdomainId=").append(domainId).
                        append("\n\tname=").append(name).toString();
    }

}