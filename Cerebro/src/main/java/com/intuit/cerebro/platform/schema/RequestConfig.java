package com.intuit.cerebro.platform.schema;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;

public class RequestConfig {
    private String baseUrl;
    private String target;
    private String companyName;
    private String market;
    private String startDate;
    private String endDate;

    private String ProductId;

    private Map<String, AttributeValue> headers;
    private Map<String, AttributeValue> params;

    public RequestConfig() {
        headers = new HashMap<>();
        params = new HashMap<>();
    }

    public String getCompanyName() { return companyName; }

    public void setCompanyName(String companyName) { this.companyName = companyName; }

    public String getMarket() { return market; }

    public void setMarket(String market) { this.market = market; }

    public String getStartDate() { return startDate; }

    public void setStartDate(String startDate) { this.startDate = startDate; }

    public String getEndDate() { return endDate; }

    public void setEndDate(String endDate) { this.endDate = endDate; }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getProductId() { return ProductId; }

    public void setProductId(String productId) { ProductId = productId; }

    public Map<String, AttributeValue> getHeaders() {
        return headers;
    }

    public Map<String, AttributeValue> getParams() {
        return params;
    }
}
