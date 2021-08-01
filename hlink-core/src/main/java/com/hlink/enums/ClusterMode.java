package com.hlink.enums;

import org.apache.commons.lang3.StringUtils;

public enum ClusterMode {
    /**
     * Applications executed in the local
     */
    local(0 , "local"),

    /**
     * Applications executed in the standalone
     */
    standalone(1, "standalone"),

    /**
     * Applications executed in the yarn session
     */
    yarnSession(2, "yarn-session"),

    /**
     * Applications executed in the yarn perjob
     */
    yarnPerJob(3, "yarn-per-job"),

    /**
     * Applications executed in the yarn application
     */
    yarnApplication(4, "yarn-application"),

    /**
     * Applications executed in the kubernetes session
     */
    kubernetesSession(5, "kubernetes-session"),

    /**
     * Applications executed in the kubernetes perjob
     */
    kubernetesPerJob(6, "kubernetes-per-job"),

    /**
     * Applications executed in the kubernetes application
     */
    kubernetesApplication(7, "kubernetes-application");

    private int type;

    private String name;

    ClusterMode(int type, String name){
        this.type = type;
        this.name = name;
    }

    public static ClusterMode getByName(String name){
        if(StringUtils.isBlank(name)){
            throw new IllegalArgumentException("ClusterMode name cannot be null or empty");
        }
        switch (name){
            case "standalone": return standalone;
            case "yarn":
            case "yarn-session": return yarnSession;
            case "yarnPer":
            case "yarn-per-job": return yarnPerJob;
            case "yarn-application": return yarnApplication;
            case "kubernetes-session": return kubernetesSession;
            case "kubernetes-per-job": return kubernetesPerJob;
            case "kubernetes-application": return kubernetesApplication;
            default: return local;
        }
    }

}
