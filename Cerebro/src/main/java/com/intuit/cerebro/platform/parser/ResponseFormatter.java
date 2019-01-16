package com.intuit.cerebro.platform.parser;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.Map;

public interface ResponseFormatter {
    String format (Map<String, AttributeValue> values) throws JsonProcessingException;
}
