package com.opensds.jsonmodels.responses.listbackends;

public class ListBackendResponse {


    private String next;

    private Backend[] backends;

    public String getNext() {
        return next;
    }

    public void setNext(String next) {
        this.next = next;
    }

    public Backend[] getBackends() {
        return backends;
    }

    public void setBackends(Backend[] backends) {
        this.backends = backends;
    }

    @Override
    public String toString() {
        return "ListBackendResponse [next = " + next + ", backends = " + backends + "]";
    }
}
