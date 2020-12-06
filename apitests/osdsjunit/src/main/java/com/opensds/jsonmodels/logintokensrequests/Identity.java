package com.opensds.jsonmodels.logintokensrequests;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

public class Identity {

    @SerializedName("methods")
    @Expose
    private List<String> methods = new ArrayList<>();
    @SerializedName("password")
    @Expose
    private Password password;

    public List<String> getMethods() {
        return methods;
    }

    public void setMethods(List<String> methods) {
        this.methods = methods;
    }

    public Password getPassword() {
        return password;
    }

    public void setPassword(Password password) {
        this.password = password;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("methods").append(methods).append("password").append(password).toString();
    }
}