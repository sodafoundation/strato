package com.opensds.jsonmodels.inputs.addbackend;

import java.util.ArrayList;

public class BackendsInputHolder
{
    private ArrayList<Backends> backends;
    private String next;

    public String getNext ()
    {
        return next;
    }

    public void setNext (String next)
    {
        this.next = next;
    }

    public ArrayList<Backends> getBackends ()
    {
        return backends;
    }

    public void setBackends (ArrayList<Backends> backends)
    {
        this.backends = backends;
    }
}