package com.opensds.jsonmodels.tokensresponses;

import java.util.List;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Token {

    @SerializedName("issued_at")
    @Expose
    String issuedAt;
    @SerializedName("audit_ids")
    @Expose
    List<String> auditIds = null;
    @SerializedName("methods")
    @Expose
    List<String> methods = null;
    @SerializedName("expires_at")
    @Expose
    String expiresAt;
    @SerializedName("user")
    @Expose
    User user;

    public String getIssuedAt() {
        return issuedAt;
    }

    public void setIssuedAt(String issuedAt) {
        this.issuedAt = issuedAt;
    }

    public List<String> getAuditIds() {
        return auditIds;
    }

    public void setAuditIds(List<String> auditIds) {
        this.auditIds = auditIds;
    }

    public List<String> getMethods() {
        return methods;
    }

    public void setMethods(List<String> methods) {
        this.methods = methods;
    }

    public String getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(String expiresAt) {
        this.expiresAt = expiresAt;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    @Override
    public String toString() {
        return "\n\tToken{" +
                "\n\t\tissuedAt='" + issuedAt +
                "\n\t\tauditIds=" + auditIds +
                "\n\t\tmethods=" + methods +
                "\n\t\texpiresAt='" + expiresAt +
                "\n\t\tuser=" + user +
                '}';
    }
}