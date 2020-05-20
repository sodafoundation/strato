package com.opensds.jsonmodels.logintokensrequests;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Domain {

    @SerializedName("name")
    @Expose
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("name").append(name).toString();
    }

}