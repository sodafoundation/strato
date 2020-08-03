package com.opensds.jsonmodels.logintokensrequests;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class AuthHolder {

    @SerializedName("auth")
    @Expose
    private Auth auth;

    public Auth getAuth() {
        return auth;
    }

    public void setAuth(Auth auth) {
        this.auth = auth;
    }

    @Override
    public String toString() {
        return "AuthHolder{" +
                "auth=" + auth +
                '}';
    }
}