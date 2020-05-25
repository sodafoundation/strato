package com.opensds.jsonmodels.inputs.createmigration;

public class DestConnInput
{
    private String bucketName;

    private String storType;

    public String getBucketName ()
    {
        return bucketName;
    }

    public void setBucketName (String bucketName)
    {
        this.bucketName = bucketName;
    }

    public String getStorType ()
    {
        return storType;
    }

    public void setStorType (String storType)
    {
        this.storType = storType;
    }

    @Override
    public String toString()
    {
        return "ClassPojo [bucketName = "+bucketName+", storType = "+storType+"]";
    }
}