package com.opensds.jsonmodels.typesresponse;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class TypesHolder {

    @SerializedName("types")
    @Expose
    private List<Type> types = null;
    @SerializedName("next")
    @Expose
    private Integer next;

    public List<Type> getTypes() {
        return types;
    }

    public void setTypes(List<Type> types) {
        this.types = types;
    }

    public Integer getNext() {
        return next;
    }

    public void setNext(Integer next) {
        this.next = next;
    }

    @Override
    public String toString() {
        return "TypesHolder{" +
                "types=" + types +
                ", next=" + next +
                '}';
    }
}