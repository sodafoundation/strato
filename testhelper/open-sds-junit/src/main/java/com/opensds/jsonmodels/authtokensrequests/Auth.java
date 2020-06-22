package com.opensds.jsonmodels.authtokensrequests;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Auth {

    @SerializedName("identity")
    @Expose
    private Identity identity;
    @SerializedName("scope")
    @Expose
    private Scope scope;

    public Identity getIdentity() {
        return identity;
    }

    public void setIdentity(Identity identity) {
        this.identity = identity;
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    @Override
    public String toString() {
        return "Auth{" +
                "identity=" + identity +
                ", scope=" + scope +
                '}';
    }
}
