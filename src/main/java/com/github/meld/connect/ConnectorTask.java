package com.github.meld.connect;

public class ConnectorTask {
    private String cluster;
    private String name;
    private String offset;
    private String properties;
    
    public ConnectorTask(String cluster, String name) {
        this.cluster = cluster;
        this.name = name;
    }

    public String getCluster() {
        return cluster;
    }
    
    public void setCluster(String cluster) {
        this.cluster = cluster;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }
}
