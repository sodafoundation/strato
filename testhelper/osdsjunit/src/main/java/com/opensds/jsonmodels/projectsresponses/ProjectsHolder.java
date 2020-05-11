package com.opensds.jsonmodels.projectsresponses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class ProjectsHolder {

    @SerializedName("links")
    @Expose
    private Links links;
    @SerializedName("projects")
    @Expose
    private List<Project> projects = null;

    public Links getLinks() {
        return links;
    }

    public void setLinks(Links links) {
        this.links = links;
    }

    public List<Project> getProjects() {
        return projects;
    }

    public void setProjects(List<Project> projects) {
        this.projects = projects;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("\n\tlinks=").append(links).
                append("\n\tprojects=").append(projects).toString();
    }

}