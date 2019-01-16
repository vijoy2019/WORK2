package com.intuit.cerebro.platform.schema;

public class CrossAppSchema {
    private String app;
    private String rundate;
    private String market;
    private String productId;
    private String category;
    private CrossApp[] crossApp;

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

    public String getCategory() { return category; }

    public void setCategory(String category) { this.category = category; }

    public CrossApp[] getCrossApp() {
        return crossApp;
    }

    public void setCrossApp(CrossApp[] crossApp) {
        this.crossApp = crossApp;
    }

}
