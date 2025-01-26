package com.company;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSnowflakeStreaming {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSnowflakeStreaming.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static SnowflakeWriter snowflakeWriter;

    public static void main(String[] args) {
        Properties config = loadConfig();
        String topicName = config.getProperty("kafka.topic.name");
        logger.info("Starting application to consume from topic: {}", topicName);
        
        // Initialize Snowflake writer
        try {
            snowflakeWriter = new SnowflakeWriter(config);
            logger.info("Successfully initialized Snowflake writer");
        } catch (Exception e) {
            logger.error("Failed to initialize Snowflake writer", e);
            System.exit(1);
        }

        StreamsBuilder builder = new StreamsBuilder();

        // Create stream from the specified topic
        KStream<String, String> inputStream = builder.stream(topicName);
        logger.info("Created Kafka stream for topic: {}", topicName);

        // Process each record
        inputStream.foreach((key, value) -> {
            try {
                logger.info("Received message - Key: {}, Value: {}", key, value);
                JsonNode jsonNode = objectMapper.readTree(value);
                snowflakeWriter.upsertRecord(jsonNode);
            } catch (Exception e) {
                logger.error("Error processing record - Key: {}, Value: {}", key, value, e);
            }
        });

        // Create and start the Kafka Streams instance
        KafkaStreams streams = new KafkaStreams(builder.build(), createKafkaConfig(config));
        
        // Add state listener
        streams.setStateListener((newState, oldState) -> {
            logger.info("Kafka Streams state changed from {} to {}", oldState, newState);
        });

        // Add uncaught exception handler
        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            logger.error("Uncaught exception in thread {}", thread.getName(), throwable);
            return KafkaStreams.State.NOT_RUNNING;
        });

        streams.start();
        logger.info("Kafka Streams application started successfully");

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down the application...");
            streams.close();
            if (snowflakeWriter != null) {
                snowflakeWriter.close();
            }
        }));
    }

    private static Properties createKafkaConfig(Properties config) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-snowflake-streaming");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("kafka.bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        
        // Add consumer configurations
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-snowflake-streaming-group");
        
        // Confluent Cloud security settings
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", config.getProperty("kafka.sasl.jaas.config"));
        
        logger.info("Kafka configuration: bootstrap.servers={}, security.protocol=SASL_SSL", 
                   config.getProperty("kafka.bootstrap.servers"));
        
        return props;
    }

    private static Properties loadConfig() {
        Properties config = new Properties();
        try {
            config.load(KafkaSnowflakeStreaming.class.getClassLoader().getResourceAsStream("application.properties"));
            logger.info("Successfully loaded configuration");
        } catch (Exception e) {
            logger.error("Could not load configuration", e);
            throw new RuntimeException("Could not load configuration", e);
        }
        return config;
    }
}
