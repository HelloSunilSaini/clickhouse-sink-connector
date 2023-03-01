package com.altinity.clickhouse.sink.connector.converters;

import com.clickhouse.client.ClickHouseDataType;
import com.google.common.io.BaseEncoding;
import io.debezium.data.Enum;
import io.debezium.data.EnumSet;
import io.debezium.data.Json;
import io.debezium.data.Xml;
import io.debezium.data.Uuid;
import io.debezium.data.geometry.Geometry;
import io.debezium.time.*;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
/**
 * Function that maps the debezium/kafka connect
 * data types to ClickHouse Data Types.
 *
 */
public class ClickHouseDataTypeMapper {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseDataTypeMapper.class.getName());

    static Map<MutablePair<Schema.Type, String>, ClickHouseDataType> dataTypesMap;

    static {
        dataTypesMap = new HashMap<>();

        // Integer
        dataTypesMap.put(new MutablePair(Schema.INT16_SCHEMA.type(), null), ClickHouseDataType.Int16);
        dataTypesMap.put(new MutablePair(Schema.INT8_SCHEMA.type(), null), ClickHouseDataType.Int8);
        dataTypesMap.put(new MutablePair(Schema.INT32_SCHEMA.type(), null), ClickHouseDataType.Int32);
        dataTypesMap.put(new MutablePair(Schema.INT64_SCHEMA.type(), null), ClickHouseDataType.Int64);

        // Float
        dataTypesMap.put(new MutablePair(Schema.FLOAT32_SCHEMA.type(), null), ClickHouseDataType.Float32);
        dataTypesMap.put(new MutablePair(Schema.FLOAT64_SCHEMA.type(), null), ClickHouseDataType.Float32);

        // String
        dataTypesMap.put(new MutablePair(Schema.STRING_SCHEMA.type(), null), ClickHouseDataType.String);

        // BLOB -> String
        dataTypesMap.put(new MutablePair(Schema.BYTES_SCHEMA.type(), Decimal.LOGICAL_NAME), ClickHouseDataType.Decimal);

        // DATE
        dataTypesMap.put(new MutablePair<>(Schema.INT32_SCHEMA.type(), Date.SCHEMA_NAME), ClickHouseDataType.Date32);

        // TIME
        dataTypesMap.put(new MutablePair<>(Schema.INT32_SCHEMA.type(), Time.SCHEMA_NAME), ClickHouseDataType.String);


        dataTypesMap.put(new MutablePair(Schema.INT32_SCHEMA.type(), org.apache.kafka.connect.data.Date.LOGICAL_NAME), ClickHouseDataType.Date32);
        dataTypesMap.put(new MutablePair(Schema.INT32_SCHEMA.type(), org.apache.kafka.connect.data.Time.LOGICAL_NAME), ClickHouseDataType.String);

        // debezium.time.MicroTime -> String (Time does not exist in CH)
        dataTypesMap.put(new MutablePair(Schema.INT64_SCHEMA.type(), MicroTime.SCHEMA_NAME), ClickHouseDataType.String);


        dataTypesMap.put(new MutablePair(Schema.INT64_SCHEMA.type(), org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME), ClickHouseDataType.DateTime64);
        dataTypesMap.put(new MutablePair(Schema.INT64_SCHEMA.type(), org.apache.kafka.connect.data.Date.LOGICAL_NAME), ClickHouseDataType.Date32);
        dataTypesMap.put(new MutablePair(Schema.INT64_SCHEMA.type(), org.apache.kafka.connect.data.Time.LOGICAL_NAME), ClickHouseDataType.String);

        // Timestamp -> DateTime
        dataTypesMap.put(new MutablePair(Schema.INT64_SCHEMA.type(), Timestamp.SCHEMA_NAME), ClickHouseDataType.DateTime64);
        // Datetime with microseconds precision
        dataTypesMap.put(new MutablePair(Schema.INT64_SCHEMA.type(), MicroTimestamp.SCHEMA_NAME), ClickHouseDataType.DateTime64);

        // BLOB -> String
        dataTypesMap.put(new MutablePair(Schema.Type.BYTES, null), ClickHouseDataType.String);

        // BYTES, BIT
        dataTypesMap.put(new MutablePair(Schema.Type.BYTES, io.debezium.data.Bits.LOGICAL_NAME), ClickHouseDataType.String);

        // Boolean -> Boolean
        dataTypesMap.put(new MutablePair<>(Schema.Type.BOOLEAN, null), ClickHouseDataType.Bool);

        // Timestamp -> ZonedTimeStamp -> DateTime
        dataTypesMap.put(new MutablePair<>(Schema.Type.STRING, ZonedTimestamp.SCHEMA_NAME), ClickHouseDataType.DateTime64);

        dataTypesMap.put(new MutablePair<>(Schema.Type.STRING, Enum.LOGICAL_NAME), ClickHouseDataType.String);

        dataTypesMap.put(new MutablePair<>(Schema.Type.STRING, Json.LOGICAL_NAME), ClickHouseDataType.String);

        dataTypesMap.put(new MutablePair<>(Schema.Type.STRING, Xml.LOGICAL_NAME), ClickHouseDataType.String);

        dataTypesMap.put(new MutablePair<>(Schema.Type.STRING, Uuid.LOGICAL_NAME), ClickHouseDataType.String);

        dataTypesMap.put(new MutablePair<>(Schema.Type.STRING, Xml.LOGICAL_NAME), ClickHouseDataType.String);

        dataTypesMap.put(new MutablePair<>(Schema.INT32_SCHEMA.type(), Year.SCHEMA_NAME), ClickHouseDataType.Int32);

        // EnumSet -> String
        dataTypesMap.put(new MutablePair<>(Schema.STRING_SCHEMA.type(), EnumSet.LOGICAL_NAME), ClickHouseDataType.String);

        dataTypesMap.put(new MutablePair<>(Schema.Type.STRUCT, null), ClickHouseDataType.String);
        // Geometry -> Geometry
        dataTypesMap.put(new MutablePair<>(Schema.Type.STRUCT, Geometry.LOGICAL_NAME), ClickHouseDataType.String);

        // Array -> Array
        dataTypesMap.put(new MutablePair<>(Schema.Type.ARRAY, null), ClickHouseDataType.Array);

    }

    /**
     * Core function that is used for converting data
     * based on the Kafka connect schema.
     * This function has the mapping logic of Kafka connect schema name/type -> ClickHouse data type.
     * @param type
     * @param schemaName
     * @param value
     * @param index
     * @param ps
     * @return true, if handled, false if the data type is not current handled.
     * @throws SQLException
     */
    public static boolean convert(Schema.Type type, String schemaName, Schema schema,
                                               Object value,
                                               int index,
                                               PreparedStatement ps) throws SQLException {

        boolean result = true;

        //TinyINT -> INT16 -> TinyInt
        boolean isFieldTinyInt = (type == Schema.INT16_SCHEMA.type());

        boolean isFieldTypeInt = (type == Schema.INT8_SCHEMA.type()) ||
                (type == Schema.INT32_SCHEMA.type());

        boolean isFieldTypeFloat = (type == Schema.FLOAT32_SCHEMA.type()) ||
                (type == Schema.FLOAT64_SCHEMA.type());


        // MySQL BigInt -> INT64
        boolean isFieldTypeBigInt = false;
        boolean isFieldTime = false;
        boolean isFieldDateTime = false;

        boolean isFieldTypeDecimal = false;

        // Decimal -> BigDecimal(JDBC)
        if (type == Schema.BYTES_SCHEMA.type() && (schemaName != null &&
                schemaName.equalsIgnoreCase(Decimal.LOGICAL_NAME))) {
            isFieldTypeDecimal = true;
        }

        if (type == Schema.INT64_SCHEMA.type()) {
            // Time -> INT64 + io.debezium.time.MicroTime
            if (schemaName != null && schemaName.equalsIgnoreCase(MicroTime.SCHEMA_NAME) ||
                (schemaName != null && schemaName.equalsIgnoreCase(org.apache.kafka.connect.data.Time.LOGICAL_NAME))
            ) {
                isFieldTime = true;
            } else if ((schemaName != null && schemaName.equalsIgnoreCase(Timestamp.SCHEMA_NAME)) ||
                    (schemaName != null && schemaName.equalsIgnoreCase(MicroTimestamp.SCHEMA_NAME)) ||
                    (schemaName != null && schemaName.equalsIgnoreCase(org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME))
                    ) {
                //DateTime -> INT64 + Timestamp(Debezium)
                // MicroTimestamp ("yyyy-MM-dd HH:mm:ss")
                isFieldDateTime = true;
            } else {
                isFieldTypeBigInt = true;
            }
        }

        // Text columns
        if (type == Schema.Type.STRING) {
            if (schemaName != null && schemaName.equalsIgnoreCase(ZonedTimestamp.SCHEMA_NAME)) {
                // MySQL(Timestamp) -> String, name(ZonedTimestamp) -> Clickhouse(DateTime)
                ps.setString(index, DebeziumConverter.ZonedTimestampConverter.convert(value));

            } else if(schemaName != null && schemaName.equalsIgnoreCase(Json.LOGICAL_NAME)) {
                // if the column is JSON, it should be written, String otherwise
                ps.setObject(index, value);
            }else {
                ps.setString(index, (String) value);
            }
        } else if (isFieldTypeInt) {
            if (schemaName != null && schemaName.equalsIgnoreCase(Date.SCHEMA_NAME)) {
                // Date field arrives as INT32 with schema name set to io.debezium.time.Date
                ps.setDate(index, DebeziumConverter.DateConverter.convert(value));

            } else if (schemaName != null && schemaName.equalsIgnoreCase(Timestamp.SCHEMA_NAME)) {
                ps.setTimestamp(index, (java.sql.Timestamp) value);
            } else {
                ps.setInt(index, (Integer) value);
            }
        } else if (isFieldTypeFloat) {
            if (value instanceof Float) {
                ps.setFloat(index, (Float) value);
            } else if (value instanceof Double) {
                ps.setDouble(index, (Double) value);
            }
        } else if (type == Schema.BOOLEAN_SCHEMA.type()) {
            ps.setBoolean(index, (Boolean) value);
        } else if (isFieldTypeBigInt || isFieldTinyInt) {
            ps.setObject(index, value);
        } else if (isFieldDateTime || isFieldTime) {
            if (isFieldDateTime) {
                if  (schemaName != null && schemaName.equalsIgnoreCase(MicroTimestamp.SCHEMA_NAME)) {
                    // Handle microtimestamp first
                    ps.setTimestamp(index, DebeziumConverter.MicroTimestampConverter.convert(value));
                }
                else if (value instanceof Long) {
                    boolean isColumnDateTime64 = false;
                    if((schemaName.equalsIgnoreCase(Timestamp.SCHEMA_NAME) || schemaName.equalsIgnoreCase(org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME)) && type == Schema.INT64_SCHEMA.type()){
                        isColumnDateTime64 = true;
                    }
                    ps.setLong(index, DebeziumConverter.TimestampConverter.convert(value, isColumnDateTime64));
                } else if (value instanceof java.util.Date) {
                    ps.setLong(index, ((java.util.Date) value).getTime());
                }
            } else if (isFieldTime) {
                ps.setString(index, DebeziumConverter.MicroTimeConverter.convert(value));
            }
            // Convert this to string.
            // ps.setString(index, String.valueOf(value));
        } else if (isFieldTypeDecimal) {
            ps.setBigDecimal(index, (BigDecimal) value);
        } else if (type == Schema.Type.BYTES) {
            // Blob storage.
            if (value instanceof byte[]) {
                String hexValue = new String((byte[]) value);
                ps.setString(index, hexValue);
            } else if (value instanceof java.nio.ByteBuffer) {
                ps.setString(index, BaseEncoding.base16().lowerCase().encode(((ByteBuffer) value).array()));
            }

        } else if (type == Schema.Type.STRUCT && schemaName.equalsIgnoreCase(Geometry.LOGICAL_NAME)) {
            // Geometry
            if (value instanceof Struct) {
                Struct geometryValue = (Struct) value;
                Object wkbValue = geometryValue.get("wkb");
                if(wkbValue != null) {
                    ps.setString(index, BaseEncoding.base16().lowerCase().encode(((ByteBuffer) wkbValue).array()));
                } else {
                    ps.setString(index, "");
                }
            } else {
                ps.setString(index, "");
            }
        } else if (type == Schema.Type.ARRAY){
            Schema valueSchema = schema.valueSchema();
            if (valueSchema.type() == Schema.Type.STRUCT) {
                ArrayList<String> jsons = new ArrayList<>();
                Map<String,Object> config = new HashMap<>();
                config.put("schemas.enable", false);
                for(int i = 0;i < ((ArrayList) value).size();i++){
                    Object obj = ((ArrayList) value).get(i);
                    org.apache.kafka.connect.json.JsonConverter jsonConverter = new org.apache.kafka.connect.json.JsonConverter();
                    jsonConverter.configure(config, false);
                    
                    byte[] bytes = jsonConverter.fromConnectData("",valueSchema,obj);
                    String s = new String(bytes);   
                    jsons.add(s);
                }
                ps.setObject(index, jsons);

            }else{
                ps.setObject(index, value);
            }
        }
        else {
            result = false;
        }

        return result;
    }

    public static ClickHouseDataType getClickHouseDataType(Schema.Type kafkaConnectType, String schemaName) {

        ClickHouseDataType matchingDataType = null;
        for (Map.Entry<MutablePair<Schema.Type, String>, ClickHouseDataType> entry : dataTypesMap.entrySet()) {
            //   return dataTypesMap.get(kafkaConnectType);

            MutablePair mp = entry.getKey();

            if((schemaName == null && mp.right == null && kafkaConnectType == mp.left)  ||
                    (kafkaConnectType == mp.left && (schemaName != null && schemaName.equalsIgnoreCase((String) mp.right))) ||
                    (kafkaConnectType == mp.left &&  mp.right == null && (schemaName != null && kafkaConnectType == Schema.Type.STRUCT))
                    ) {
                // Founding matching type.
                matchingDataType = entry.getValue();
            }

        }

        return matchingDataType;
    }
}
