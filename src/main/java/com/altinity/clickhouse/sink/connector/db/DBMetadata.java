package com.altinity.clickhouse.sink.connector.db;

import com.clickhouse.jdbc.ClickHouseConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import static com.altinity.clickhouse.sink.connector.db.ClickHouseDbConstants.CHECK_DB_EXISTS_SQL;

public class DBMetadata {

    private static final Logger log = LoggerFactory.getLogger(DBMetadata.class);


    public enum TABLE_ENGINE {
        COLLAPSING_MERGE_TREE("CollapsingMergeTree"),

        REPLICATED_COLLAPSING_MERGE_TREE("ReplicatedCollapsingMergeTree"),

        REPLACING_MERGE_TREE("ReplacingMergeTree"),

        REPLICATED_REPLACING_MERGE_TREE("ReplicatedReplacingMergeTree"),

        MERGE_TREE("MergeTree"),

        DEFAULT("default");

        private final String engine;

        public String getEngine() {
            return engine;
        }

        TABLE_ENGINE(String engine) {
            this.engine = engine;
        }
    }

    /**
     * Wrapper function to get table engine.
     * @param conn
     * @param tableName
     * @return
     */
    public MutablePair<TABLE_ENGINE, String> getTableEngine(ArrayList<ClickHouseConnection> connections, String databaseName, String tableName) {

        MutablePair<TABLE_ENGINE, String> result;
        int i = 0;
        do{
            result = getTableEngineUsingSystemTables(connections.get(i), databaseName, tableName);
            if(result.left == null) {
                result = getTableEngineUsingShowTable(connections.get(i), tableName);
            }
            if (result.left != null){
                break;
            }
            i++;
        }while(i<connections.size());
        return result;
    }

    /**
     * Function to check if database exists by querying the information schema tables.
     * @param conn
     * @param databaseName
     * @return
     */
    public boolean checkIfDatabaseExists(ClickHouseConnection conn, String databaseName) throws SQLException {

        boolean result = false;
        try (Statement stmt = conn.createStatement()) {
            String showSchemaQuery = String.format(CHECK_DB_EXISTS_SQL, databaseName);
            ResultSet rs = stmt.executeQuery(showSchemaQuery);
            if (rs.next()) {
                String response = rs.getString(1);
                if(response.equalsIgnoreCase(databaseName)) {
                    result = true;
                }
            }

            rs.close();
        } catch(SQLException se) {
            log.error("checkIfDatabaseExists exception", se);
            // ToDO: For some reason, this query throws SQLException when DB is not available.

            //throw se;
        }

        return result;
    }

    /**
     * Function to return Engine type for table.
     * This function calls the "create table" SQL
     * to get the schema of the table.
     * @param tableName
     * @return
     */
    public MutablePair<TABLE_ENGINE, String> getTableEngineUsingShowTable(ClickHouseConnection conn, String tableName) {
        MutablePair<TABLE_ENGINE, String> result = new MutablePair<>();

        try {
            if (conn == null) {
                log.error("Error with DB connection");
                return new MutablePair<>(null, null);
            }
            try(Statement stmt = conn.createStatement()) {
                String showSchemaQuery = String.format("show create table %s", tableName);
                ResultSet rs = stmt.executeQuery(showSchemaQuery);
                if(rs.next()) {
                    String response =  rs.getString(1);
                    if(response.contains(TABLE_ENGINE.COLLAPSING_MERGE_TREE.engine)) {
                        result.left = TABLE_ENGINE.COLLAPSING_MERGE_TREE;
                        result.right = getSignColumnForCollapsingMergeTree(response);
                    } else if (response.contains(TABLE_ENGINE.REPLICATED_COLLAPSING_MERGE_TREE.engine)){
                        result.left = TABLE_ENGINE.REPLICATED_COLLAPSING_MERGE_TREE;
                        result.right = getSignColumnForCollapsingMergeTree(response);
                    } else if(response.contains(TABLE_ENGINE.REPLACING_MERGE_TREE.engine)) {
                        result.left = TABLE_ENGINE.REPLACING_MERGE_TREE;
                        result.right = getVersionColumnForReplacingMergeTree(response);
                    } else if(response.contains(TABLE_ENGINE.MERGE_TREE.engine)) {
                        result.left = TABLE_ENGINE.MERGE_TREE;
                    }else {
                        result.left = TABLE_ENGINE.DEFAULT;
                    }
                }
                rs.close();
                stmt.close();
                log.info("ResultSet" + rs);
            }
        } catch(Exception e) {
            log.error("getTableEngineUsingShowTable exception", e);
        }

        return result;
    }

    public static final String COLLAPSING_MERGE_TREE_SIGN_PREFIX = "CollapsingMergeTree(";
    public static final String REPLICATED_COLLAPSING_MERGE_TREE_SIGN_PREFIX = "ReplicatedCollapsingMergeTree(";
    public static final String REPLACING_MERGE_TREE_VER_PREFIX = "ReplacingMergeTree(";

    public static final String REPLICATED_REPLACING_MERGE_TREE_VER_PREFIX = "ReplicatedReplacingMergeTree(";
    /**
     * Function to extract the sign column for CollapsingMergeTree
     * @param createDML
     * @return Sign column
     */
    public String getSignColumnForCollapsingMergeTree(String createDML) {

        String signColumn = "sign";

        if(createDML.contains(TABLE_ENGINE.COLLAPSING_MERGE_TREE.getEngine())) {
            signColumn = StringUtils.substringBetween(createDML, COLLAPSING_MERGE_TREE_SIGN_PREFIX, ")");
        } else if (createDML.contains(TABLE_ENGINE.REPLICATED_COLLAPSING_MERGE_TREE.getEngine())){
            String parameters = StringUtils.substringBetween(createDML, REPLICATED_COLLAPSING_MERGE_TREE_SIGN_PREFIX, ")");
            if(parameters != null) {
                String[] parameterArray = parameters.split(",");
                if(parameterArray != null && parameterArray.length >= 3) {
                    signColumn = parameterArray[2].trim();
                }
            }
        } else {
            log.error("Error: Trying to retrieve sign from table that is not CollapsingMergeTree");
        }

        return signColumn;
    }

    /**
     * Function to extract the version column for ReplacingMergeTree
     * @param createDML
     * @return Sign column
     */
    public String getVersionColumnForReplacingMergeTree(String createDML) {

        String versionColumn = "ver";

        if(createDML.contains(TABLE_ENGINE.REPLICATED_REPLACING_MERGE_TREE.getEngine())) {
            String parameters = StringUtils.substringBetween(createDML, REPLICATED_REPLACING_MERGE_TREE_VER_PREFIX, ")");
            if(parameters != null) {
                String[] parameterArray = parameters.split(",");
                if(parameterArray != null && parameterArray.length >= 3) {
                    versionColumn = parameterArray[2].trim();
                }
            }
        }
        else if(createDML.contains(TABLE_ENGINE.REPLACING_MERGE_TREE.getEngine())) {
            versionColumn = StringUtils.substringBetween(createDML, REPLACING_MERGE_TREE_VER_PREFIX, ")").trim();
        } else {
            log.error("Error: Trying to retrieve ver from table that is not ReplacingMergeTree");
        }

        return versionColumn;
    }
    /**
     * Function to get table engine using system tables.
     * @param conn ClickHouse Connection
     * @param tableName Table Name.
     * @return TABLE_ENGINE type
     */
    public MutablePair<TABLE_ENGINE, String> getTableEngineUsingSystemTables(final ClickHouseConnection conn, final String database,
                                                        final String tableName) {
        MutablePair<TABLE_ENGINE, String> result = new MutablePair<>();


        try {
            if (conn == null) {
                log.error("Error with DB connection");
                return result;
            }
            try(Statement stmt = conn.createStatement()) {
                String showSchemaQuery = String.format("select engine_full from system.tables where name='%s' and database='%s'",
                        tableName, database);
                ResultSet rs = stmt.executeQuery(showSchemaQuery);
                if(rs.next()) {
                    String response =  rs.getString(1);
                    result = getEngineFromResponse(response);
                }
                rs.close();
                stmt.close();
                log.info("ResultSet" + rs);
            }
        } catch(Exception e) {
            log.error("getTableEngineUsingSystemTables exception", e);
        }

        return result;
    }

    public MutablePair<TABLE_ENGINE, String> getEngineFromResponse(String response) {
        MutablePair<TABLE_ENGINE, String> result = new MutablePair<>();

        if(response.contains(TABLE_ENGINE.COLLAPSING_MERGE_TREE.engine)) {
            result.left = TABLE_ENGINE.COLLAPSING_MERGE_TREE;
            result.right = getSignColumnForCollapsingMergeTree(response);
        } else if (response.contains(TABLE_ENGINE.REPLICATED_COLLAPSING_MERGE_TREE.engine)){
            result.left = TABLE_ENGINE.REPLICATED_COLLAPSING_MERGE_TREE;
            result.right = getSignColumnForCollapsingMergeTree(response);
        } else if(response.contains(TABLE_ENGINE.REPLACING_MERGE_TREE.engine)) {
            result.left = TABLE_ENGINE.REPLACING_MERGE_TREE;
            result.right = getVersionColumnForReplacingMergeTree(response);
        } else if(response.contains(TABLE_ENGINE.MERGE_TREE.engine)) {
            result.left = TABLE_ENGINE.MERGE_TREE;
        } else {
            result.left = TABLE_ENGINE.DEFAULT;
        }

        return result;
    }
}
