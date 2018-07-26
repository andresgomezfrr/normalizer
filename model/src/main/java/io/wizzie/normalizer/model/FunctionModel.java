package io.wizzie.normalizer.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FunctionModel {
    private String className;
    private Map<String, Object> properties;
    private List<String> stores;
    private String name;

    @JsonCreator
    public FunctionModel(@JsonProperty("name") String name,
                         @JsonProperty("className") String className,
                         @JsonProperty("properties") Map<String, Object> properties,
                         @JsonProperty("stores") List<String> stores) {
        this.name = name;
        if (name == null) this.name = className;
        this.className = className;
        if (properties != null) {
            this.properties = properties;
        } else {
            this.properties = new HashMap<>();
        }
        this.stores = stores;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getClassName() {
        return className;
    }

    @JsonProperty
    public Map<String, Object> getProperties() {
        return properties;
    }

    @JsonProperty
    public List<String> getStores() {
        return stores;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("className: ").append(className).append(", ")
                .append("properties: ").append(properties).append(", ")
                .append("stores: ").append(stores)
                .append("}");

        return builder.toString();
    }
}
