package com.opensds.jsonmodels.authtokensrequests;


import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

public class Identity {

    @SerializedName("methods")
    @Expose
    private List<String> methods = new ArrayList<>();
    @SerializedName("token")
    @Expose
    private Token token;

    public List<String> getMethods() {
        return methods;
    }

    public void setMethods(List<String> methods) {
        this.methods = methods;
    }

    public Token getToken() {
        return token;
    }

    public void setToken(Token token) {
        this.token = token;
    }

    @Override
    public String toString() {
        return "Identity{" +
                "methods=" + methods +
                ", token=" + token +
                '}';
    }
}