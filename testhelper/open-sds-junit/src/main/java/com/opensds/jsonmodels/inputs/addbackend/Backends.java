package com.opensds.jsonmodels.inputs.addbackend;

public class Backends
{
    private String bucketName;

    private String endpoint;

    private String tenantId;

    private String name;

    private String id;

    private String type;

    private String region;

    private String userId;

    public String getBucketName ()
    {
        return bucketName;
    }

    public void setBucketName (String bucketName)
    {
        this.bucketName = bucketName;
    }

    public String getEndpoint ()
    {
        return endpoint;
    }

    public void setEndpoint (String endpoint)
    {
        this.endpoint = endpoint;
    }

    public String getTenantId ()
    {
        return tenantId;
    }

    public void setTenantId (String tenantId)
    {
        this.tenantId = tenantId;
    }

    public String getName ()
    {
        return name;
    }

    public void setName (String name)
    {
        this.name = name;
    }

    public String getId ()
    {
        return id;
    }

    public void setId (String id)
    {
        this.id = id;
    }

    public String getType ()
    {
        return type;
    }

    public void setType (String type)
    {
        this.type = type;
    }

    public String getRegion ()
    {
        return region;
    }

    public void setRegion (String region)
    {
        this.region = region;
    }

    public String getUserId ()
    {
        return userId;
    }

    public void setUserId (String userId)
    {
        this.userId = userId;
    }
}