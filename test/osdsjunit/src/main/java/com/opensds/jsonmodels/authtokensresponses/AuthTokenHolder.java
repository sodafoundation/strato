package com.opensds.jsonmodels.authtokensresponses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class AuthTokenHolder {

    String responseHeaderSubjectToken;

    @SerializedName("token")
    @Expose
    private Token token;

    public Token getToken() {
        return token;
    }

    public void setToken(Token token) {
        this.token = token;
    }

    @Override
    public String toString() {
        return "AuthTokenHolder{" +
                "responseHeaderSubjectToken='" + responseHeaderSubjectToken + '\'' +
                ", token=" + token +
                '}';
    }

    public String getResponseHeaderSubjectToken() {
        return responseHeaderSubjectToken;
    }

    public void setResponseHeaderSubjectToken(String responseHeaderSubjectToken) {
        this.responseHeaderSubjectToken = responseHeaderSubjectToken;
    }
}
