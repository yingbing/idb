package idb.nosql;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NoSQLDatabase {

    private static final Logger logger = LoggerFactory.getLogger(NoSQLDatabase.class);

    private Map<String, String> storage = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final File dataFile = new File("data.json");

    public NoSQLDatabase() {
        loadFromDisk();
    }

    // 原有的 put 方法
    public void put(String key, String value) {
        storage.put(key, value);
        saveToDisk();
    }

    // 原有的 get 方法
    public String get(String key) {
        return storage.get(key);
    }

    // 原有的 delete 方法
    public void delete(String key) {
        storage.remove(key);
        saveToDisk();
    }

    // 新增的 putJson 方法
    public void putJson(String key, Object value) {
        try {
            String jsonValue = objectMapper.writeValueAsString(value);
            put(key, jsonValue);
        } catch (IOException e) {
            logger.error("putJson exception", e);
        }
    }

    // 新增的 getAsObject 方法
    public <T> T getAsObject(String key, Class<T> clazz) {
        String jsonValue = get(key);
        if (jsonValue != null) {
            try {
                return objectMapper.readValue(jsonValue, clazz);
            } catch (IOException e) {
                logger.error("getAsObject class exception", e);
            }
        }
        return null;
    }

    // 使用 TypeReference 的 getAsObject 方法
    public <T> T getAsObject(String key, TypeReference<T> typeReference) {
        String jsonValue = get(key);
        if (jsonValue != null) {
            try {
                return objectMapper.readValue(jsonValue, typeReference);
            } catch (IOException e) {
                logger.error("getAsObject type exception", e);
            }
        }
        return null;
    }

    // 保存数据到磁盘
    private void saveToDisk() {
        try {
            // 检查文件父目录是否存在，如果不存在则创建
            if (!dataFile.exists()) {
                File parentDir = dataFile.getParentFile();
                if (parentDir != null && !parentDir.exists()) {
                    if(parentDir.mkdirs()) { // 创建父目录
                        logger.info("create dir:" + parentDir);
                    }
                }
                if (dataFile.createNewFile()) { // 创建文件
                    logger.info("create file:" + dataFile);
                }
            }

            objectMapper.writeValue(dataFile, storage);
        } catch (IOException e) {
            logger.error("saveToDisk exception", e);
        }
    }

    // 从磁盘加载数据
    private void loadFromDisk() {
        if (dataFile.exists()) {
            try {
                storage = objectMapper.readValue(dataFile, new TypeReference<ConcurrentHashMap<String, String>>(){});
            } catch (IOException e) {
                logger.error("loadFromDisk exception", e);
            }
        }
    }

    // 查询所有数据
    public Map<String, String> queryAll() {
        return new ConcurrentHashMap<>(storage);
    }
}
