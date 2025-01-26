package com.company;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class SnowflakeWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SnowflakeWriter.class);
    private final Connection connection;
    private final String tableName;
    private final BlockingQueue<JsonNode> batchQueue;
    private final int batchSize;
    private final Thread batchProcessor;
    private volatile boolean running = true;

    public SnowflakeWriter(Properties config) {
        try {
            this.tableName = config.getProperty("snowflake.table.name");
            this.batchSize = Integer.parseInt(config.getProperty("snowflake.batch.size", "100"));
            this.batchQueue = new ArrayBlockingQueue<>(batchSize * 2);

            // Initialize Snowflake connection
            connection = DriverManager.getConnection(
                config.getProperty("snowflake.jdbc.url"),
                config.getProperty("snowflake.user"),
                config.getProperty("snowflake.password")
            );

            // Start batch processor thread
            batchProcessor = new Thread(this::processBatch);
            batchProcessor.start();
            
            logger.info("SnowflakeWriter initialized for table: {}", tableName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize SnowflakeWriter", e);
        }
    }

    public void upsertRecord(JsonNode record) throws InterruptedException {
        logger.info("Received record: {}", record.toString());
        batchQueue.put(record);
    }

    private void processBatch() {
        while (running) {
            try {
                if (batchQueue.size() > 0) {
                    JsonNode record = batchQueue.take();
                    processRecord(record);
                } else {
                    Thread.sleep(100); // Prevent busy-waiting
                }
            } catch (Exception e) {
                logger.error("Error processing record for table " + tableName, e);
            }
        }
    }

    private void processRecord(JsonNode record) throws SQLException {
        String operation = record.path("operation").asText();
        logger.info("Processing {} operation for record: {}", operation, record.toString());

        // For DELETE operations, get ID from 'before' section
        String customerId;
        if ("DELETE".equalsIgnoreCase(operation)) {
            customerId = record.path("before").path("customer_id").asText();
            logger.info("DELETE operation for customer_id: {} from 'before' section", customerId);
        } else {
            // For INSERT/UPDATE operations, get ID from 'after' section
            customerId = record.path("after").path("customer_id").asText();
            logger.info("INSERT/UPDATE operation for customer_id: {} from 'after' section", customerId);
        }

        if (customerId == null || customerId.isEmpty()) {
            logger.error("Customer ID is null or empty in record: {}", record);
            return;
        }

        switch (operation.toUpperCase()) {
            case "INSERT":
            case "UPDATE":
                String upsertSQL = String.format(
                    "MERGE INTO %s TARGET " +
                    "USING (SELECT CAST(? AS VARCHAR) as ID, PARSE_JSON(?) as DATA) AS SOURCE " +
                    "ON TARGET.ID = SOURCE.ID " +
                    "WHEN MATCHED THEN UPDATE SET " +
                    "TARGET.DATA = SOURCE.DATA, " +
                    "TARGET.LAST_UPDATED = CURRENT_TIMESTAMP() " +
                    "WHEN NOT MATCHED THEN INSERT (ID, DATA, LAST_UPDATED) " +
                    "VALUES (SOURCE.ID, SOURCE.DATA, CURRENT_TIMESTAMP())",
                    tableName
                );

                try (PreparedStatement pstmt = connection.prepareStatement(upsertSQL)) {
                    pstmt.setString(1, customerId);
                    pstmt.setString(2, record.toString());
                    int rowsAffected = pstmt.executeUpdate();
                    logger.info("Successfully executed MERGE for {} operation. Customer ID: {}. Rows affected: {}", 
                              operation, customerId, rowsAffected);
                    
                    // Verify the update
                    String verifySQL = String.format("SELECT DATA, LAST_UPDATED FROM %s WHERE ID = ?", tableName);
                    try (PreparedStatement verifyStmt = connection.prepareStatement(verifySQL)) {
                        verifyStmt.setString(1, customerId);
                        ResultSet rs = verifyStmt.executeQuery();
                        if (rs.next()) {
                            logger.info("After {}, record state - ID: {}, DATA: {}, LAST_UPDATED: {}", 
                                      operation, customerId, rs.getString("DATA"), rs.getTimestamp("LAST_UPDATED"));
                        }
                    }
                }
                break;

            case "DELETE":
                String deleteSQL = String.format("DELETE FROM %s WHERE ID = ?", tableName);
                try (PreparedStatement pstmt = connection.prepareStatement(deleteSQL)) {
                    pstmt.setString(1, customerId);
                    int rowsAffected = pstmt.executeUpdate();
                    logger.info("Successfully processed DELETE operation for customer ID: {}. Rows affected: {}", 
                              customerId, rowsAffected);
                    
                    // Verify deletion
                    String verifySQL = String.format("SELECT COUNT(*) FROM %s WHERE ID = ?", tableName);
                    try (PreparedStatement verifyStmt = connection.prepareStatement(verifySQL)) {
                        verifyStmt.setString(1, customerId);
                        ResultSet rs = verifyStmt.executeQuery();
                        if (rs.next()) {
                            int count = rs.getInt(1);
                            logger.info("After DELETE, found {} records with ID: {}", count, customerId);
                        }
                    }
                }
                break;

            default:
                logger.warn("Unknown operation type: {} for customer ID: {}", operation, customerId);
        }
    }

    @Override
    public void close() {
        logger.info("Closing SnowflakeWriter for table: {}", tableName);
        running = false;
        try {
            batchProcessor.join(5000);
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (Exception e) {
            logger.error("Error closing SnowflakeWriter", e);
        }
    }
}
