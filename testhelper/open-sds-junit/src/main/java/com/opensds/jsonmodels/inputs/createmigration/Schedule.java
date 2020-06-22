package com.opensds.jsonmodels.inputs.createmigration;

public class Schedule
{
    private String type;

    private String tiggerProperties;

    public String getType ()
    {
        return type;
    }

    public void setType (String type)
    {
        this.type = type;
    }

    public String getTiggerProperties ()
    {
        return tiggerProperties;
    }

    public void setTiggerProperties (String tiggerProperties)
    {
        this.tiggerProperties = tiggerProperties;
    }

    @Override
    public String toString()
    {
        return "ClassPojo [type = "+type+", tiggerProperties = "+tiggerProperties+"]";
    }
}