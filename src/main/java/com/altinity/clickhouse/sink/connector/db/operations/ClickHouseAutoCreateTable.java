package com.altinity.clickhouse.sink.connector.db.operations;

import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.jdbc.ClickHouseConnection;
import org.apache.kafka.connect.data.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

import static com.altinity.clickhouse.sink.connector.db.ClickHouseDbConstants.*;
import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfigVariables;
import com.altinity.clickhouse.sink.connector.db.DBMetadata.*;

/**
 * Class that wraps all functionality
 * related to creating tables
 * from kafka sink record.
 */
public class ClickHouseAutoCreateTable extends ClickHouseTableOperationsBase{


    private final ClickHouseSinkConnectorConfig config;

    private static final Logger log = LoggerFactory.getLogger(ClickHouseAutoCreateTable.class.getName());

    public ClickHouseAutoCreateTable(ClickHouseSinkConnectorConfig config)  {
        this.config = config;
    }

    public void createNewTable(ArrayList<String> primaryKey, String tableName, Field[] fields, ArrayList<ClickHouseConnection> connections) throws SQLException {
        Map<String, String> colNameToDataTypeMap = this.getColumnNameToCHDataTypeMapping(fields);
        String createTableQuery = this.createTableSyntax(primaryKey, tableName, fields, colNameToDataTypeMap);
        // ToDO: need to run it before a session is created.
        log.info("**** AUTO CREATE TABLE " + createTableQuery);
        for (int i=0;i<connections.size();i++){
            this.runQuery(createTableQuery, connections.get(i));
        }
    }

    /**
     * Function to generate CREATE TABLE for ClickHouse.
     *
     * @param primaryKey
     * @param columnToDataTypesMap
     * @return CREATE TABLE query
     */
    public java.lang.String createTableSyntax(ArrayList<String> primaryKey, String tableName, Field[] fields, Map<String, String> columnToDataTypesMap) {

        StringBuilder createTableSyntax = new StringBuilder();

        createTableSyntax.append(CREATE_TABLE).append(" ").append(tableName).append(" on CLUSTER '{cluster}' (");

        for(Field f: fields) {
            String colName = f.name();
            String dataType = columnToDataTypesMap.get(colName);
            // boolean isNull = false;
            // if(f.schema().isOptional() == true) {
            //     isNull = true;
            // }
            createTableSyntax.append("`").append(colName).append("` ").append(dataType);

            // Ignore setting NULL OR not NULL for JSON.
            if(dataType != null && dataType.equalsIgnoreCase(ClickHouseDataType.JSON.name())) {
                // ignore adding nulls;
                createTableSyntax.append(" ").append(NULL);
            } else if (dataType.contains("Array")){
                createTableSyntax.append("");
            }else {
                if (primaryKey != null && primaryKey.contains(colName)) {
                    createTableSyntax.append(" ").append(NOT_NULL);
                } else {
                    createTableSyntax.append(" ").append(NULL);
                }
            }
            createTableSyntax.append(",");

        }
//        for(Map.Entry<String, String>  entry: columnToDataTypesMap.entrySet()) {
//            createTableSyntax.append("`").append(entry.getKey()).append("`").append(" ").append(entry.getValue()).append(",");
//        }
        //createTableSyntax.deleteCharAt(createTableSyntax.lastIndexOf(","));

        // Append sign and version columns

        if (this.config.getString(ClickHouseSinkConnectorConfigVariables.CLICKHOUSE_TABLE_ENGINE).trim().equalsIgnoreCase(TABLE_ENGINE.REPLICATED_MERGE_TREE.getEngine())){
            createTableSyntax.deleteCharAt(createTableSyntax.length()-1);  
            createTableSyntax.append(") ");
            createTableSyntax.append("ENGINE = ReplicatedMergeTree(").append("'/clickhouse/{cluster}/tables/{shard}/{database}/{table}', '{replica}') ");
        } else {
            createTableSyntax.append("`").append(SIGN_COLUMN).append("` ").append(SIGN_COLUMN_DATA_TYPE).append(" ").append(NULL).append(",");
            createTableSyntax.append("`").append(VERSION_COLUMN).append("` ").append(VERSION_COLUMN_DATA_TYPE);
            createTableSyntax.append(") ");
            createTableSyntax.append("ENGINE = ReplicatedReplacingMergeTree(").append("'/clickhouse/{cluster}/tables/{shard}/{database}/{table}', ").append("'{replica}', ").append(VERSION_COLUMN).append(") ");
        }
        if(primaryKey != null && !(this.config.getString(ClickHouseSinkConnectorConfigVariables.CLICKHOUSE_TABLE_ENGINE).trim().equalsIgnoreCase(TABLE_ENGINE.REPLICATED_MERGE_TREE.getEngine()))) {
            createTableSyntax.append(PRIMARY_KEY).append("(");
            createTableSyntax.append(primaryKey.stream().map(Object::toString).collect(Collectors.joining(",")));
            createTableSyntax.append(") ");

            createTableSyntax.append(ORDER_BY).append("(");
            createTableSyntax.append(primaryKey.stream().map(Object::toString).collect(Collectors.joining(",")));
            createTableSyntax.append(")");
        } else {
            // ToDO:
            createTableSyntax.append(ORDER_BY_TUPLE);
        }
       return createTableSyntax.toString();
    }
}
