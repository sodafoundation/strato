package com.opensds.jsonmodels.akskresponses;

public class Blob {


    private String access;

    private String secret;


    public String getAccess() {
        return access;
    }


    public void setAccess(String access) {
        this.access = access;
    }


    public String getSecret() {
        return secret;
    }


    public void setSecret(String secret) {
        this.secret = secret;
    }

    @Override
    public String toString() {
        return "Blob{" +
                "access='" + access + '\'' +
                ", secret='" + secret + '\'' +
                '}';
    }
}
