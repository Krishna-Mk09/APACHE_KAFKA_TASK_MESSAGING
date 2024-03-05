package com.solix.com.consumerservice_two.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class KafkaMessageListener {

    private final JdbcTemplate jdbcTemplate;

    @Autowired 
    public KafkaMessageListener(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @KafkaListener(topics = "updatedTopic")
    public void listen(String message) {
        System.out.println("Received message from updated topic: " + message);

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> jsonData = objectMapper.readValue(message, Map.class);

            String tableName = "updatedTopicTable";
            String schema = "consumertwo";

            createTableIfNotExists(tableName, schema, jsonData);
            insertData(tableName, jsonData);

            System.out.println("Data inserted into the database successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createTableIfNotExists(String tableName, String schema, Map<String, Object> jsonData) {
        String createTableQuery = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s)",
                schema, tableName,
                jsonData.keySet().stream().map(key -> "`" + key + "` TEXT").collect(Collectors.joining(", ")));

        jdbcTemplate.execute(createTableQuery);
    }

    private void insertData(String tableName, Map<String, Object> jsonData) {
        String insertQuery = String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableName,
                String.join(", ", jsonData.keySet().stream().map(key -> "`" + key + "`").collect(Collectors.toList())),
                String.join(", ", jsonData.values().stream().map(value -> "'" + value + "'").collect(Collectors.toList())));

        jdbcTemplate.execute(insertQuery);
    }
}
