package com.opensds.jsonmodels.authtokensresponses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Endpoint {

    @SerializedName("region_id")
    @Expose
    private String regionId;
    @SerializedName("url")
    @Expose
    private String url;
    @SerializedName("region")
    @Expose
    private String region;
    @SerializedName("interface")
    @Expose
    private String _interface;
    @SerializedName("id")
    @Expose
    private String id;

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getInterface() {
        return _interface;
    }

    public void setInterface(String _interface) {
        this._interface = _interface;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Endpoint{" +
                "regionId='" + regionId + '\'' +
                ", url='" + url + '\'' +
                ", region='" + region + '\'' +
                ", _interface='" + _interface + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}