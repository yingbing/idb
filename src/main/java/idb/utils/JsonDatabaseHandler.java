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

    public void addJsonRecord(Object jsonData) throws IOException, ReflectiveOperationException {
        String className = jsonData.getClass().getSimpleName();
        TableMetadata tableMetadata = metadataMap.get(className);
        if (tableMetadata != null) {
            saveRecord(jsonData, tableMetadata, null);
        }
    }

    public <T> T getJsonRecord(int id, Class<T> valueType) throws IOException, ReflectiveOperationException {
        String className = valueType.getSimpleName();
        TableMetadata tableMetadata = metadataMap.get(className);
        if (tableMetadata != null) {
            return loadRecord(id, valueType, tableMetadata);
        }
        return null;
    }

    private void saveRecord(Object jsonData, TableMetadata tableMetadata, Integer parentId) throws IOException, ReflectiveOperationException {
        Table table = database.getTable(tableMetadata.getTableName());
        int generatedId = UUID.randomUUID().hashCode();
        setFieldValue(jsonData, "id", generatedId);
        Record record = new Record(generatedId);
        for (Map.Entry<String, String> entry : tableMetadata.getFields().entrySet()) {
            String fieldName = entry.getKey();
            String fieldType = entry.getValue();
            Object fieldValue = getFieldValue(jsonData, fieldName);
            if (fieldType.startsWith("List<")) {
                List<?> list = (List<?>) fieldValue;
                for (Object item : list) {
                    saveRecord(item, metadataMap.get(fieldType.substring(5, fieldType.length() - 1)), generatedId);
                }
            } else if (metadataMap.containsKey(fieldType)) {
                setFieldValue(fieldValue, "parentId", generatedId);
                saveRecord(fieldValue, metadataMap.get(fieldType), generatedId);
            } else {
                record.setData(fieldName, fieldValue);
            }
        }
        if (parentId != null) {
            record.setData("parentId", parentId);
            setFieldValue(jsonData, "parentId", parentId);
        }
        table.addRecord(record);
    }

    private <T> T loadRecord(int id, Class<T> valueType, TableMetadata tableMetadata) throws IOException, ReflectiveOperationException {
        Table table = database.getTable(tableMetadata.getTableName());
        Record record = table.getRecord(id);
        if (record == null) {
            return null;
        }
        T instance = valueType.getDeclaredConstructor().newInstance();
        setFieldValue(instance, "id", id);
        for (Map.Entry<String, String> entry : tableMetadata.getFields().entrySet()) {
            String fieldName = entry.getKey();
            String fieldType = entry.getValue();
            if (fieldType.startsWith("List<")) {
                String nestedClassName = fieldType.substring(5, fieldType.length() - 1);
                Class<?> nestedClass = Class.forName("idb.model." + nestedClassName);
                List<Object> nestedList = loadNestedList(id, nestedClass, metadataMap.get(nestedClassName));
                setFieldValue(instance, fieldName, nestedList);
            } else if (metadataMap.containsKey(fieldType)) {
                Table table1 = database.getTable( metadataMap.get(fieldType).getTableName());
                Map<Integer, Record> nestedRecords = table1.queryRecords("parentId", id);
                if (!nestedRecords.isEmpty()) {
                    Record nestedRecord = nestedRecords.values().iterator().next();
                    Object nestedInstance = loadRecord(nestedRecord.getId(), Class.forName("idb.model." + fieldType), metadataMap.get(fieldType));
                    setFieldValue(instance, fieldName, nestedInstance);
                }
            } else {
                setFieldValue(instance, fieldName, record.getData(fieldName));
            }
        }
        return instance;
    }

    private List<Object> loadNestedList(int parentId, Class<?> nestedClass, TableMetadata nestedMetadata) throws ReflectiveOperationException, IOException {
        Table table = database.getTable(nestedMetadata.getTableName());
        Map<Integer, Record> records = table.queryRecords("parentId", parentId);
        List<Object> resultList = new ArrayList<>();
        for (Record record : records.values()) {
            Object nestedInstance = loadRecord(record.getId(), nestedClass, nestedMetadata);
            resultList.add(nestedInstance);
        }
        return resultList;
    }

    private Object getFieldValue(Object instance, String fieldName) throws ReflectiveOperationException {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(instance);
    }

    private void setFieldValue(Object instance, String fieldName, Object value) throws ReflectiveOperationException {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(instance, value);
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
