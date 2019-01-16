package com.intuit.cerebro.platform.schema;

public class CrossApp {
    private String cross_app_product_code;
    private String cross_app_name;
    private String cross_app_parent_company_name;
    private String rank;
    private String affinity;
    private String cross_app_usage;

    public String getCross_app_product_code() {
        return cross_app_product_code;
    }

    public void setCross_app_product_code(String cross_app_product_code) { this.cross_app_product_code = cross_app_product_code; }

    public String getCross_app_name() {
        return cross_app_name;
    }

    public void setCross_app_name(String cross_app_name) {
        this.cross_app_name = cross_app_name;
    }

    public String getCross_app_parent_company_name() {
        return cross_app_parent_company_name;
    }

    public void setCross_app_parent_company_name(String cross_app_parent_company_name) { this.cross_app_parent_company_name = cross_app_parent_company_name; }

    public String getRank() {
        return rank;
    }

    public void setRank(String rank) {
        this.rank = rank;
    }

    public String getAffinity() {
        return affinity;
    }

    public void setAffinity(String affinity) {
        this.affinity = affinity;
    }

    public String getCross_app_usage() {
        return cross_app_usage;
    }

    public void setCross_app_usage(String cross_app_usage) {
        this.cross_app_usage = cross_app_usage;
    }

}
