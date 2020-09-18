package com.opensds.jsonmodels.inputs.createbucket;

public class CreateBucketFileInput {

    private String xmlPayload;
    private String xmlRequestTrue;
    private String xmlRequestFalse;

    public String getXmlRequestFalse() {
        return xmlRequestFalse;
    }

    public void setXmlRequestFalse(String xmlRequestFalse) {
        this.xmlRequestFalse = xmlRequestFalse;
    }

    public String getXmlRequestTrue() {
        return xmlRequestTrue;
    }

    public void setXmlRequestTrue(String xmlRequestTrue) {
        this.xmlRequestTrue = xmlRequestTrue;
    }

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
