package com.opensds.jsonmodels.akskresponses;

public class SignatureKey {
    String secretAccessKey;
    String dateStamp;
    String dayDate;
    String regionName;
    String serviceName;
    String AccessKey;

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public void setSecretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
    }

    public String getDateStamp() {
        return dateStamp;
    }

    public void setDateStamp(String dateStamp) {
        this.dateStamp = dateStamp;
    }

    public String getDayDate() {
        return dayDate;
    }

    public void setDayDate(String dayDate) {
        this.dayDate = dayDate;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getAccessKey() {
        return AccessKey;
    }

    public void setAccessKey(String accessKey) {
        AccessKey = accessKey;
    }

    @Override
    public String toString() {
        return "SignatureKey{" +
                "secretAccessKey='" + secretAccessKey + '\'' +
                ", dateStamp='" + dateStamp + '\'' +
                ", dayDate='" + dayDate + '\'' +
                ", regionName='" + regionName + '\'' +
                ", serviceName='" + serviceName + '\'' +
                ", AccessKey='" + AccessKey + '\'' +
                '}';
    }
}
