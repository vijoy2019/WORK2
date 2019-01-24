package com.company;

public class UsageSchema {

    private String app;
    private String rundate;
    private String market;
    private String productId;
    private String active_users;
    private String avg_sessions_per_user;
    private String avg_time_per_user;
    private String usage_penetration;
    private String install_penetration;
    private String install_base;

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getRundate() {
        return rundate;
    }

    public void setRundate(String rundate) {
        this.rundate = rundate;
    }

    public String getMarket() {
        return market;
    }

    public void setMarket(String market) {
        this.market = market;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getActive_users() {
        return active_users;
    }

    public void setActive_users(String active_users) {
        this.active_users = active_users;
    }

    public String getAvg_sessions_per_user() {
        return avg_sessions_per_user;
    }

    public void setAvg_sessions_per_user(String avg_sessions_per_user) { this.avg_sessions_per_user = avg_sessions_per_user; }

    public String getAvg_time_per_user() {
        return avg_time_per_user;
    }

    public void setAvg_time_per_user(String avg_time_per_user) {
        this.avg_time_per_user = avg_time_per_user;
    }

    public String getUsage_penetration() {
        return usage_penetration;
    }

    public void setUsage_penetration(String usage_penetration) {
        this.usage_penetration = usage_penetration;
    }

    public String getInstall_penetration() {
        return install_penetration;
    }

    public void setInstall_penetration(String install_penetration) {
        this.install_penetration = install_penetration;
    }

    public String getInstall_base() {
        return install_base;
    }

    public void setInstall_base(String install_base) {
        this.install_base = install_base;
    }
}
