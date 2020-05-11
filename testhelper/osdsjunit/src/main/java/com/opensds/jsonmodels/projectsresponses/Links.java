package com.opensds.jsonmodels.projectsresponses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Links {

    @SerializedName("self")
    @Expose
    private String self;
    @SerializedName("previous")
    @Expose
    private Object previous;
    @SerializedName("next")
    @Expose
    private Object next;

    public String getSelf() {
        return self;
    }

    public void setSelf(String self) {
        this.self = self;
    }

    public Object getPrevious() {
        return previous;
    }

    public void setPrevious(Object previous) {
        this.previous = previous;
    }

    public Object getNext() {
        return next;
    }

    public void setNext(Object next) {
        this.next = next;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("\n\tself=").append(self).
                append("\n\tprevious=").append(previous).
                append("\n\tnext=").append(next).toString();
    }

}