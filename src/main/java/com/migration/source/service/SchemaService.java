package com.migration.source.service;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Service
public class SchemaService {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private SchemaRegistryClient schemaRegistryClient;



    public void sendDataToKafka(String topic, String tableName) throws Exception {
        // Generate Avro schema from database metadata
        Schema avroSchema = generateAvroSchema(tableName);

        int schemaId = schemaRegistryClient.register(tableName + "-value-schema", avroSchema);


        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers); // Kafka broker address
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", schemaRegistryUrl); // Schema Registry address
        Producer<String, Object> producer = new KafkaProducer<>(props);

        List<Map<String, Object>> data = fetchDataFromDatabase(tableName);


        // Send data to Kafka with Avro schema
        for (Map<String, Object> row : data) {
            GenericRecord avroRecord = new GenericData.Record(avroSchema);

            // Iterate over each field in the Avro schema
            for (Schema.Field field : avroSchema.getFields()) {
                String fieldName = field.name();
                Object fieldValue = row.get(fieldName); // Get the corresponding value from the row

                // Convert field value to match Avro schema data type
                Object avroValue = convertToAvroType(field.schema(), fieldValue);

                avroRecord.put(fieldName, avroValue);
            }
            System.out.println(new ProducerRecord<>(topic, avroRecord));
            producer.send(new ProducerRecord<>(topic, avroRecord));
        }

        producer.close();
    }

    private Schema generateAvroSchema(String tableName) throws SQLException {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(tableName).fields();
        try (var connection = dataSource.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet resultSet = metadata.getColumns(null, null, tableName, null);



            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                String columnType = resultSet.getString("TYPE_NAME");
                System.out.println(columnName + columnType);
                // Map JDBC types to Avro types and add fields to the Avro schema
                switch (columnType.toUpperCase()) {
                    case "SERIAL":
                    case "INTEGER":
                    case "SMALLINT":
                    case "TINYINT":
                    case "INT":
                        fieldAssembler = fieldAssembler.name(columnName).type().intType().noDefault();
                        break;
                    case "VARCHAR":
                    case "CHAR":
                    case "NVARCHAR":
                    case "NCHAR":
                        fieldAssembler = fieldAssembler.name(columnName).type().stringType().noDefault();
                        break;
                    case "BIGINT":
                        fieldAssembler = fieldAssembler.name(columnName).type().longType().noDefault();
                        break;
                    case "BOOLEAN":
                        fieldAssembler = fieldAssembler.name(columnName).type().booleanType().noDefault();
                        break;
                    case "TIMESTAMP":
                        fieldAssembler = fieldAssembler.name(columnName).type().longType().noDefault();
                        break;
                    case "NUMERIC":
                    case "DECIMAL":
                        fieldAssembler = fieldAssembler.name(columnName).type().doubleType().noDefault();
                        break;
                    case "TEXT":
                        fieldAssembler = fieldAssembler.name(columnName).type().stringType().noDefault();
                        break;
                    default:
                        fieldAssembler = fieldAssembler.name(columnName).type().stringType().noDefault();
                        break;
                }
            }

            return fieldAssembler.endRecord();
        }
    }


    private List<Map<String, Object>> fetchDataFromDatabase(String tableName) {
        String query = "SELECT * FROM " + tableName;

        return jdbcTemplate.queryForList(query);
    }

    private Object convertToAvroType(Schema schema, Object value) {
        if (value == null) {
            return null;
        }

        switch (schema.getType()) {
            case INT:
                return ((Number) value).intValue();
            case LONG:
                return ((Number) value).longValue();
            case FLOAT:
                return ((Number) value).floatValue();
            case DOUBLE:
                return ((Number) value).doubleValue();
            case BOOLEAN:
                return (boolean) value;
            case STRING:
                return value.toString();
            default:
                throw new IllegalArgumentException("Unsupported Avro type: " + schema.getType());
        }
    }


    public List<String> getAllTables() throws Exception {
        List<String> tables = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet resultSet = metaData.getTables(null, null, null, new String[]{"TABLE"})) {
                while (resultSet.next()) {
                    tables.add(resultSet.getString("TABLE_NAME"));
                }
            }
        } catch (Exception e) {
            throw new Exception("Failed to retrieve tables from database", e);
        }
        return tables;
    }

}