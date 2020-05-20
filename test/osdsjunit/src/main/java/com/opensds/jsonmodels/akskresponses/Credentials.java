package com.opensds.jsonmodels.akskresponses;

import com.google.gson.Gson;

public class Credentials
{
    private String blob;
    public Blob blobObj;

    private String user_id;

    private String project_id;

    private Links links;

    private String id;

    private String type;

    public String getblob ()
    {
        return blob;
    }

    public void setblob (String blob)
    {
        this.blob = blob;
        Gson gson = new Gson();
        this.blobObj = gson.fromJson(this.blob, Blob.class);
        System.out.println(this.blobObj);
    }

    public String getUser_id ()
    {
        return user_id;
    }

    public void setUser_id (String user_id)
    {
        this.user_id = user_id;
    }

    public String getProject_id ()
    {
        return project_id;
    }

    public void setProject_id (String project_id)
    {
        this.project_id = project_id;
    }

    public Links getLinks ()
    {
        return links;
    }

    public void setLinks (Links links)
    {
        this.links = links;
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

    @Override
    public String toString()
    {
        //System.out.println(blobObj);
        return "Credential [blob = "+blobObj+", user_id = "+user_id+", project_id = "+project_id+", links = "+links+", id = "+id+", type = "+type+"]";
    }

    public Blob getBlobObj ()
    {
        return blobObj;
    }
}