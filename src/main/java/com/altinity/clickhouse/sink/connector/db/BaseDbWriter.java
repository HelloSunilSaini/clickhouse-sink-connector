package com.altinity.clickhouse.sink.connector.db;

import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import com.altinity.clickhouse.sink.connector.db.operations.ClickHouseCreateDatabase;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;

import org.apache.kafka.common.protocol.types.Field.Str;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Properties;

public class BaseDbWriter {

    // protected ClickHouseConnection conn;
    protected ArrayList<ClickHouseConnection> connections = new ArrayList<ClickHouseConnection>();

    private static final Logger log = LoggerFactory.getLogger(BaseDbWriter.class);

    public BaseDbWriter(
            String hostNames,
            Integer port,
            String database,
            String userName,
            String password,
            Boolean enableSsl,
            ClickHouseSinkConnectorConfig config
    ) {
        // split hostname with commas
        String[] hostNameStrings = hostNames.split(",",0);
        tryCreatingDB(hostNameStrings,port,database,userName,password,enableSsl);
        for (int i=0; i<hostNameStrings.length; i++){
            String connectionUrl = getConnectionString((String)hostNameStrings[i], port, database, enableSsl);
            this.createConnection(connectionUrl, "Agent_" + i, userName, password);
        }
    }

    public ArrayList<ClickHouseConnection> getConnection() {
        return this.connections;
    }
    public String getConnectionString(String hostName, Integer port, String database, Boolean enableSsl) {
        if (enableSsl){
            return String.format("jdbc:clickhouse://%s:%s/%s?ssl=true", hostName, port, database);
        }
        return String.format("jdbc:clickhouse://%s:%s/%s", hostName, port, database);
    }

    public void tryCreatingDB(String[] hostNameStrings,Integer port,String database,String userName,String password,Boolean enableSsl){
        for (int i=0; i<hostNameStrings.length; i++){
            ClickHouseConnection conn;
            try {
                String connectionUrl = getConnectionStringWithOutDB((String)hostNameStrings[i], port, enableSsl);
                Properties properties = new Properties();
                properties.setProperty("client_name", "DB_CreateAgent");
                properties.setProperty("custom_settings", "allow_experimental_object_type=1");
                ClickHouseDataSource dataSource = new ClickHouseDataSource(connectionUrl, properties);
                conn = dataSource.getConnection(userName, password);
                DBMetadata metadata = new DBMetadata();
                if(false == metadata.checkIfDatabaseExists(conn, database)) {
                    try {
                        new ClickHouseCreateDatabase().createNewDatabase(conn, database);
                    } catch (Exception e) {
                        log.error("Error creating ClickHouse database" + e);
                    }
                }
            } catch (Exception e) {
                log.error("Error creating ClickHouse connection" + e);
            }
        }
    }

    public String getConnectionStringWithOutDB(String hostName, Integer port, Boolean enableSsl) {
        if (enableSsl){
            return String.format("jdbc:clickhouse://%s:%s?ssl=true", hostName, port);
        }
        return String.format("jdbc:clickhouse://%s:%s", hostName, port);
    }

    /**
     * Function to create Connection using the JDBC Driver
     *
     * @param url        url with the JDBC format jdbc:ch://localhost/test
     * @param clientName Client Name
     * @param userName   UserName
     * @param password   Password
     */
    protected void createConnection(String url, String clientName, String userName, String password) {
        try {
            Properties properties = new Properties();
            properties.setProperty("client_name", clientName);
            properties.setProperty("custom_settings", "allow_experimental_object_type=1");
            ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
            ClickHouseConnection conn = dataSource.getConnection(userName, password);
            this.connections.add(conn);
        } catch (Exception e) {
            log.error("Error creating ClickHouse connection" + e);
        }
    }

    /**
     * Function that uses the DatabaseMetaData JDBC functionality
     * to get the column name and column data type as key/value pair.
     */
    public Map<String, String> getColumnsDataTypesForTable(String tableName) {

        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        try {
            if (this.connections.size() == 0) {
                log.error("Error with DB connection");
                return result;
            }
            // TODO : add check for DB name 
            ResultSet columns = this.connections.get(0).getMetaData().getColumns(null, null,
                    tableName, null);
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String typeName = columns.getString("TYPE_NAME");

//                Object dataType = columns.getString("DATA_TYPE");
//                String columnSize = columns.getString("COLUMN_SIZE");
//                String isNullable = columns.getString("IS_NULLABLE");
//                String isAutoIncrement = columns.getString("IS_AUTOINCREMENT");

                result.put(columnName, typeName);
            }
        } catch (SQLException sq) {
            log.error("Exception retrieving Column Metadata", sq);
        }
        return result;
    }
}

