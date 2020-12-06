package com.opensds.jsonmodels.tokensresponses;

import com.google.gson.annotations.SerializedName;

public class Domain {

    public @SerializedName("id") String id;
    public @SerializedName("name") String name;

    public Domain(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public String getid() {
        return id;
    }

    public void setid(String id) {
        this.id = id;
    }

    public String getname() {
        return name;
    }

    public void setname(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "\n\tDomain{" +
                "\n\t\tid='" + id + '\'' +
                "\n\t\tname='" + name + '\'' +
                '}';
    }
}