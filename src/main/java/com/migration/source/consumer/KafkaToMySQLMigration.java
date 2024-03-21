package com.migration.source.consumer;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

@Component
public class KafkaToMySQLMigration {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.schema-registry-url}")
    private String schemaRegistryUrl;


    @Autowired
    private DataSource dataSource;

    public void migrateDataFromKafkaToMySQL(String kafkaTopic, String tablename) {
        try {
            SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);
            int schemaId = getSchemaIdFromRegistry(schemaRegistry, kafkaTopic);
            Schema schema = schemaRegistry.getByID(schemaId);
            System.out.println("scjema reg"+schema);
            createMySQLTable(schema, tablename);

            KafkaConsumer<String, GenericRecord> kafkaConsumer = createKafkaConsumer();
            kafkaConsumer.subscribe(Collections.singletonList(kafkaTopic));

            while (true) {
                ConsumerRecords<String, GenericRecord> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.println("record deser"+ record);
                    insertRecordIntoMySQL(record.value(), tablename);
                }
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    private int getSchemaIdFromRegistry(SchemaRegistryClient schemaRegistry, String topic) throws IOException, RestClientException {
        // Fetch the latest schema version for the given topic
        SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(topic + "-value");

        // Get the schema ID from the schema metadata
        return schemaMetadata.getId();
    }

    private void createMySQLTable(Schema schema, String tableName) throws SQLException {
        StringBuilder sql = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");

        for (Schema.Field field : schema.getFields()) {
            String columnName = field.name();
            Schema.Type fieldType = field.schema().getType();
            int sqlType = mapAvroTypeToSQLType(fieldType);

            sql.append(columnName).append(" ").append(getSQLType(sqlType));

            if (sqlType == Types.VARCHAR) {
                int length = field.schema().getProp("max_length") != null ? Integer.parseInt(field.schema().getProp("max_length")) : 255;
                sql.append("(").append(length).append(")");
            }

            sql.append(",");
        }

        sql.setCharAt(sql.length() - 1, ')'); // Replace the last comma with a closing parenthesis

        try {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            jdbcTemplate.execute(sql.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private KafkaConsumer<String, GenericRecord> createKafkaConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "prod");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", schemaRegistryUrl);
        return new KafkaConsumer<>(props);
    }



    private void insertRecordIntoMySQL(GenericRecord record, String table) throws SQLException {
        StringBuilder sql = new StringBuilder("INSERT INTO "+ table+ "(");

        // Append column names to the SQL statement
        for (Schema.Field field : record.getSchema().getFields()) {
            String fieldName = field.name();
            sql.append(fieldName).append(",");
        }

        // Remove the trailing comma and close the SQL statement
        sql.deleteCharAt(sql.length() - 1); // Remove the last comma
        sql.append(") VALUES (");

        // Append placeholders for values in the SQL statement
        for (Schema.Field field : record.getSchema().getFields()) {
            sql.append("?,"); // Use JDBC placeholders for prepared statement
        }

        // Remove the trailing comma and close the SQL statement
        sql.deleteCharAt(sql.length() - 1); // Remove the last comma
        sql.append(")");

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            // Set values from the Avro record into the prepared statement
            int i = 1;
            for (Schema.Field field : record.getSchema().getFields()) {
                Object value = record.get(field.name());
                System.out.println("sql value"+sql);
                setStatementParameter(statement, i++, value);
            }

            statement.executeUpdate();
        }
    }

    private void setStatementParameter(PreparedStatement statement, int index, Object value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.NULL);
            return;
        }

        if (value instanceof Integer) {
            statement.setInt(index, (Integer) value);
        } else if (value instanceof Long) {
            statement.setLong(index, (Long) value);
        } else if (value instanceof Float) {
            statement.setFloat(index, (Float) value);
        } else if (value instanceof Double) {
            statement.setDouble(index, (Double) value);
        } else if (value instanceof Boolean) {
            statement.setBoolean(index, (Boolean) value);
        } else if (value instanceof String) {
            statement.setString(index, (String) value);
        } else if (value instanceof byte[]) {
            statement.setBytes(index, (byte[]) value);
        } else if (value instanceof org.apache.avro.util.Utf8) {
            // Convert Utf8 instance to Java string
            statement.setString(index, value.toString());
        } else {
            // Handle unsupported data types accordingly
            throw new IllegalArgumentException("Unsupported data type: " + value.getClass());
        }
    }



    private int mapAvroTypeToSQLType(Schema.Type avroType) {
        switch (avroType) {
            case INT:
                return Types.INTEGER;
            case LONG:
                return Types.BIGINT;
            case STRING:
                return Types.VARCHAR;
            case BOOLEAN:
                return Types.BOOLEAN;
            case DOUBLE:
                return Types.DOUBLE;
            // Handle other Avro types as needed
            default:
                throw new IllegalArgumentException("Unsupported Avro type: " + avroType);
        }
    }

    private String getSQLType(int sqlType) {
        switch (sqlType) {
            case Types.INTEGER:
                return "INT";
            case Types.BIGINT:
                return "BIGINT";
            case Types.VARCHAR:
                return "VARCHAR";
            case Types.BOOLEAN:
                return "BOOLEAN";
            //case Types.DOUBLE:
              //  return "DOUBLE";
            case Types.DOUBLE:
                return "DOUBLE PRECISION";
            // Handle other SQL types as needed
            default:
                throw new IllegalArgumentException("Unsupported SQL type: " + sqlType);
        }
    }
}
