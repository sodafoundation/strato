package com.opensds.jsonmodels.authtokensrequests;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Project {

    @SerializedName("name")
    @Expose
    private String name;
    @SerializedName("domain")
    @Expose
    private Domain domain;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Domain getDomain() {
        return domain;
    }

    public void setDomain(Domain domain) {
        this.domain = domain;
    }

    @Override
    public String toString() {
        return "Project{" +
                "name='" + name + '\'' +
                ", domain=" + domain +
                '}';
    }
}
