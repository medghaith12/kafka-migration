package com.migration.source.service;


import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class MongoService {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private SchemaRegistryClient schemaRegistryClient;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    public void sendDataToKafka(String topic, String collectionName) throws Exception {
        // Get MongoDB collection
        MongoDatabase database = mongoTemplate.getDb();
        MongoCollection<Document> collection = database.getCollection(collectionName);
        // Generate Avro schema from sample document
        Schema avroSchema = generateAvroSchema(collectionName);

        // Register Avro schema in Schema Registry
        int schemaId = schemaRegistryClient.register(collectionName + "-value-schema-mongo", avroSchema);

        // Create Kafka producer with Avro serializer
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", schemaRegistryUrl);
        Producer<String, Object> producer = new KafkaProducer<>(props);

        // Fetch data from MongoDB collection
        FindIterable<Document> documents = collection.find();

        // Send data to Kafka with Avro schema
        for (Document document : documents) {

            GenericRecord avroRecord = convertToAvroRecord(avroSchema, document);
            System.out.println("mongo record "+avroRecord);
            producer.send(new ProducerRecord<>(topic, avroRecord));
        }

        producer.close();
    }

    private Schema generateAvroSchema(String collectionName) {
        Document sampleDocument = mongoTemplate.findOne(new Query(), Document.class, collectionName);
        if (sampleDocument == null) {
            throw new IllegalArgumentException("No documents found in collection");
        }

        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(collectionName).fields();
        for (String fieldName : sampleDocument.keySet()) {
            Object fieldValue = sampleDocument.get(fieldName);
            Schema fieldSchema = mapMongoTypeToAvroSchema(fieldValue);
            fieldAssembler = fieldAssembler.name(fieldName).type(fieldSchema).noDefault();
        }

        return fieldAssembler.endRecord();
    }

    private Schema mapMongoTypeToAvroSchema(Object value) {
        if (value == null) {
            return Schema.create(Schema.Type.NULL);
        } else if (value instanceof Integer || value instanceof Long) {
            return Schema.create(Schema.Type.LONG);
        } else if (value instanceof Double) {
            return Schema.create(Schema.Type.DOUBLE);
        } else if (value instanceof Boolean) {
            return Schema.create(Schema.Type.BOOLEAN);
        } else if (value instanceof String) {
            return Schema.create(Schema.Type.STRING);
        } else {
            return Schema.create(Schema.Type.STRING); // Handle unknown types as strings
        }
    }

    private GenericRecord convertToAvroRecord(Schema schema, Document document) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        for (String fieldName : document.keySet()) {
            Object fieldValue = document.get(fieldName);
            if (fieldName.equals("_id") && fieldValue instanceof ObjectId) {
                fieldValue = ((ObjectId) fieldValue).toString();
            }
            avroRecord.put(fieldName, fieldValue);
        }
        return avroRecord;
    }
}
