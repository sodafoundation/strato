package com.opensds.jsonmodels.tokensresponses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class TokenHolder {

    String responseHeaderSubjectToken;

    @SerializedName("token")
    @Expose
    Token token;

    public Token getToken() {
        return token;
    }

    public void setToken(Token token) {
        this.token = token;
    }

    @Override
    public String toString() {
        return "\n\tTokenHolder{" +
                "\n\t\ttoken=" + token +
                "\n\treqHeaderToken=" + responseHeaderSubjectToken;

    }

    public String getResponseHeaderSubjectToken() {
        return responseHeaderSubjectToken;
    }

    public void setResponseHeaderSubjectToken(String responseHeaderSubjectToken) {
        this.responseHeaderSubjectToken = responseHeaderSubjectToken;
    }
}