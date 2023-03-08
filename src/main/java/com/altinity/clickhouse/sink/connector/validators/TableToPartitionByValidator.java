package com.altinity.clickhouse.sink.connector.validators;

import com.altinity.clickhouse.sink.connector.common.Utils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class TableToPartitionByValidator implements ConfigDef.Validator {

    public TableToPartitionByValidator() {
    }

    /**
     * ensureValid is called by framework to ensure the validity
     * 1. when connector is started or
     * 2. when validate REST API is called
     *
     * @param name
     * @param value
     */
    public void ensureValid(String name, Object value) {
        String s = (String) value;
        if (s == null || s.isEmpty()) {
            // Value is optional and therefore empy is pretty valid
            return;
        }
        try {
            if (Utils.parseTableToPartitionByMap(s) == null) {
                throw new ConfigException(name, value, "Format: <table-1>:toYYYYMM(created_at),<table-2>:toYYYYMM(created_on),...");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String toString() {
        return "Topic to table map format : comma-separated tuples, e.g. <table-1>:toYYYYMM(created_at),<table-2>:toYYYYMM(created_on),... ";
    }
}