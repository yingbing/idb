package idb.utils;

import idb.core.Database;
import idb.core.Table;
import idb.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;


public class JsonDatabaseHandler {
    private Database database;
    private ObjectMapper objectMapper;
    private Map<String, TableMetadata> metadataMap;

    public Database getDatabase() {
        return database;
    }

    public void setDatabase(Database database) {
        this.database = database;
    }

    public JsonDatabaseHandler(Database database, String configFilePath) throws IOException {
        this.database = database;
        this.objectMapper = new ObjectMapper();
        this.metadataMap = loadConfig(configFilePath);
    }

    private Map<String, TableMetadata> loadConfig(String configFilePath) throws IOException {
        Yaml yaml = new Yaml();
        try (FileInputStream inputStream = new FileInputStream(configFilePath)) {
            Map<String, Map<String, Object>> config = yaml.load(inputStream);
            Map<String, TableMetadata> metadataMap = new HashMap<>();
            for (Map.Entry<String, Map<String, Object>> entry : config.entrySet()) {
                String className = entry.getKey();
                Map<String, Object> tableConfig = entry.getValue();
                String tableName = (String) tableConfig.get("table");
                Map<String, String> fields = (Map<String, String>) tableConfig.get("fields");
                TableMetadata tableMetadata = new TableMetadata(tableName, fields);
                metadataMap.put(className, tableMetadata);
            }
            return metadataMap;
        }
    }

    public void addJsonRecord(Map<String, Object> jsonData, String rootType) throws IOException, ReflectiveOperationException {
        TableMetadata tableMetadata = metadataMap.get(rootType);
        if (tableMetadata != null) {
            saveRecord(jsonData, tableMetadata, null);
        }
    }

    public Map<String, Object> getJsonRecord(int id, String rootType) throws IOException, ReflectiveOperationException {
        TableMetadata tableMetadata = metadataMap.get(rootType);
        if (tableMetadata != null) {
            return loadRecord(id, tableMetadata);
        }
        return null;
    }

    public void updateJsonRecord(int id, Map<String, Object> jsonData, String rootType) throws IOException, ReflectiveOperationException {
        TableMetadata tableMetadata = metadataMap.get(rootType);
        if (tableMetadata != null) {
            Table table = database.getTable(tableMetadata.getTableName());
            Record record = table.getRecord(id);
            if (record != null) {
                updateRecord(jsonData, tableMetadata, id);
            } else {
                throw new IOException("Record with ID " + id + " not found.");
            }
        }
    }

    private void saveRecord(Map<String, Object> jsonData, TableMetadata tableMetadata, Integer parentId) throws IOException, ReflectiveOperationException {
        Table table = database.getTable(tableMetadata.getTableName());
        int generatedId = UUID.randomUUID().hashCode();
        jsonData.put("id", generatedId);
        Record record = new Record(generatedId);
        for (Map.Entry<String, String> entry : tableMetadata.getFields().entrySet()) {
            String fieldName = entry.getKey();
            String fieldType = entry.getValue();
            Object fieldValue = jsonData.get(fieldName);
            if (fieldType.startsWith("List<")) {
                List<Map<String, Object>> list = (List<Map<String, Object>>) fieldValue;
                for (Map<String, Object> item : list) {
                    saveRecord(item, metadataMap.get(fieldType.substring(5, fieldType.length() - 1)), generatedId);
                }
            } else if (metadataMap.containsKey(fieldType)) {
                ((Map<String, Object>) fieldValue).put("parentId", generatedId);
                saveRecord((Map<String, Object>) fieldValue, metadataMap.get(fieldType), generatedId);
            } else {
                record.setData(fieldName, fieldValue);
            }
        }
        if (parentId != null) {
            record.setData("parentId", parentId);
            jsonData.put("parentId", parentId);
        }
        table.addRecord(record);
    }

    private void updateRecord(Map<String, Object> jsonData, TableMetadata tableMetadata, int id) throws IOException, ReflectiveOperationException {
        Table table = database.getTable(tableMetadata.getTableName());
        Record record = table.getRecord(id);
        for (Map.Entry<String, String> entry : tableMetadata.getFields().entrySet()) {
            String fieldName = entry.getKey();
            String fieldType = entry.getValue();
            Object fieldValue = jsonData.get(fieldName);
            if (fieldType.startsWith("List<")) {
                List<Map<String, Object>> list = (List<Map<String, Object>>) fieldValue;
                for (Map<String, Object> item : list) {
                    Table nestedTable = database.getTable(metadataMap.get(fieldType.substring(5, fieldType.length() - 1)).getTableName());
                    Map<Integer, Record> nestedRecords = nestedTable.queryRecords("parentId", id);
                    if (!nestedRecords.isEmpty()) {
                        Record nestedRecord = nestedRecords.values().iterator().next();
                        updateRecord(item, metadataMap.get(fieldType.substring(5, fieldType.length() - 1)), nestedRecord.getId());
                    } else {
                        saveRecord(item, metadataMap.get(fieldType.substring(5, fieldType.length() - 1)), id);
                    }
                }
            } else if (metadataMap.containsKey(fieldType)) {
                Table nestedTable = database.getTable(metadataMap.get(fieldType).getTableName());
                Map<Integer, Record> nestedRecords = nestedTable.queryRecords("parentId", id);
                if (!nestedRecords.isEmpty()) {
                    Record nestedRecord = nestedRecords.values().iterator().next();
                    updateRecord((Map<String, Object>) fieldValue, metadataMap.get(fieldType), nestedRecord.getId());
                } else {
                    saveRecord((Map<String, Object>) fieldValue, metadataMap.get(fieldType), id);
                }
            } else {
                record.setData(fieldName, fieldValue);
            }
        }
        table.updateRecord(record);
    }

    private Map<String, Object> loadRecord(int id, TableMetadata tableMetadata) throws IOException, ReflectiveOperationException {
        Table table = database.getTable(tableMetadata.getTableName());
        Record record = table.getRecord(id);
        if (record == null) {
            return null;
        }
        Map<String, Object> instance = new HashMap<>();
        instance.put("id", id);
        for (Map.Entry<String, String> entry : tableMetadata.getFields().entrySet()) {
            String fieldName = entry.getKey();
            String fieldType = entry.getValue();
            if (fieldType.startsWith("List<")) {
                String nestedClassName = fieldType.substring(5, fieldType.length() - 1);
                List<Map<String, Object>> nestedList = loadNestedList(id, nestedClassName);
                instance.put(fieldName, nestedList);
            } else if (metadataMap.containsKey(fieldType)) {
                Table nestedTable = database.getTable(metadataMap.get(fieldType).getTableName());
                Map<Integer, Record> nestedRecords = nestedTable.queryRecords("parentId", id);
                if (!nestedRecords.isEmpty()) {
                    Record nestedRecord = nestedRecords.values().iterator().next();
                    Map<String, Object> nestedInstance = loadRecord(nestedRecord.getId(), metadataMap.get(fieldType));
                    instance.put(fieldName, nestedInstance);
                }
            } else {
                instance.put(fieldName, record.getData(fieldName));
            }
        }
        return instance;
    }

    private List<Map<String, Object>> loadNestedList(int parentId, String nestedClassName) throws ReflectiveOperationException, IOException {
        Table table = database.getTable(metadataMap.get(nestedClassName).getTableName());
        Map<Integer, Record> records = table.queryRecords("parentId", parentId);
        List<Map<String, Object>> resultList = new ArrayList<>();
        for (Record record : records.values()) {
            Map<String, Object> nestedInstance = loadRecord(record.getId(), metadataMap.get(nestedClassName));
            resultList.add(nestedInstance);
        }
        return resultList;
    }
}

class TableMetadata {
    private String tableName;
    private Map<String, String> fields;

    public TableMetadata(String tableName, Map<String, String> fields) {
        this.tableName = tableName;
        this.fields = fields;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, String> getFields() {
        return fields;
    }
}
