package com.opensds.jsonmodels.tokensresponses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class User {

    @SerializedName("password_expires_at")
    @Expose
    Object passwordExpiresAt;
    @SerializedName("domain")
    @Expose
    Domain domainHolder;
    @SerializedName("id")
    @Expose
    String id;
    @SerializedName("name")
    @Expose
    String name;

    public Object getPasswordExpiresAt() {
        return passwordExpiresAt;
    }

    public void setPasswordExpiresAt(Object passwordExpiresAt) {
        this.passwordExpiresAt = passwordExpiresAt;
    }

    public Domain  getDomainHolder() {
        return domainHolder;
    }

    public void setDomainHolder(Domain domainHolder) {
        this.domainHolder = domainHolder;
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
        return "\n\tUser{" +
                "\n\t\tpasswordExpiresAt=" + passwordExpiresAt +
                "\n\t\tdomain=" + domainHolder +
                "\n\t\tid='" + id + '\'' +
                "\n\t\tname='" + name + '\'' +
                '}';
    }
}