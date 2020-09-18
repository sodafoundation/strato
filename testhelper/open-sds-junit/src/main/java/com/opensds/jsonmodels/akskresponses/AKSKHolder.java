package com.opensds.jsonmodels.akskresponses;

import com.google.gson.Gson;

public class AKSKHolder {

    private Credentials[] credentials;

    private Links links;

    public Credentials[] getCredentials() {
        return credentials;
    }

    public void setCredentials(Credentials[] credentials) {
        this.credentials = credentials;
    }

    public Links getLinks() {
        return links;
    }

    public void setLinks(Links links) {
        this.links = links;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        StringBuffer sb = new StringBuffer();
        sb.append("AKSKHolder [credentials = ");
        for (Credentials c : credentials) {
            c.blobObj = gson.fromJson(c.getblob(), Blob.class);
        }
        for (Credentials c : credentials) {
            sb.append("\n\t");
            sb.append(c);
        }
        sb.append(", \nlinks = " + links + "]");
        return sb.toString();
    }
}
