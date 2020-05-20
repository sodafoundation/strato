package com.opensds.jsonmodels.inputs.createbucket;

public class CreateBucketFileInput {

    private String xmlPayload;

    public String getXmlPayload() {
        return xmlPayload;
    }

    public void setXmlPayload(String xmlPayload) {
        this.xmlPayload = xmlPayload;
    }

    @Override
    public String toString() {
        return "CreateBucketFileInput [xmlPayload = " + xmlPayload + "]";
    }
}
