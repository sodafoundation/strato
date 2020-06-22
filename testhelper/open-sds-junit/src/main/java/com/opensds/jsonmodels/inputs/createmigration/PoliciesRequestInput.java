package com.opensds.jsonmodels.inputs.createmigration;

public class PoliciesRequestInput
{
    private Schedule schedule;

    private String name;

    private String description;

    private String tenant;

    public Schedule getSchedule ()
    {
        return schedule;
    }

    public void setSchedule (Schedule schedule)
    {
        this.schedule = schedule;
    }

    public String getName ()
    {
        return name;
    }

    public void setName (String name)
    {
        this.name = name;
    }

    public String getDescription ()
    {
        return description;
    }

    public void setDescription (String description)
    {
        this.description = description;
    }

    public String getTenant ()
    {
        return tenant;
    }

    public void setTenant (String tenant)
    {
        this.tenant = tenant;
    }

    @Override
    public String toString()
    {
        return "ClassPojo [schedule = "+schedule+", name = "+name+", description = "+description+", tenant = "+tenant+"]";
    }
}