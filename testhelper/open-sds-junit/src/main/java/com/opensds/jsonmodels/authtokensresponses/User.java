package com.opensds.jsonmodels.authtokensresponses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class User {

    @SerializedName("password_expires_at")
    @Expose
    private Object passwordExpiresAt;
    @SerializedName("domain")
    @Expose
    private Domain_ domain;
    @SerializedName("id")
    @Expose
    private String id;
    @SerializedName("name")
    @Expose
    private String name;

    public Object getPasswordExpiresAt() {
        return passwordExpiresAt;
    }

    public void setPasswordExpiresAt(Object passwordExpiresAt) {
        this.passwordExpiresAt = passwordExpiresAt;
    }

    public Domain_ getDomain() {
        return domain;
    }

    public void setDomain(Domain_ domain) {
        this.domain = domain;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "User{" +
                "passwordExpiresAt=" + passwordExpiresAt +
                ", domain=" + domain +
                ", id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
