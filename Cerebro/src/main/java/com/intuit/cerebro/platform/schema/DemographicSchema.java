package com.intuit.cerebro.platform.schema;

public class DemographicSchema {

    private String app;
    private String rundate;
    private String market;
    private String productId;
    private Demographic[] demographics;

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

    public Demographic[] getDemographics() {
        return demographics;
    }

    public void setDemographics(Demographic[] demographics) {
        this.demographics = demographics;
    }
}
