package idb;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import idb.core.Database;
import idb.core.Table;
import idb.utils.JsonDatabaseHandler;
import idb.model.Address;
import idb.model.Contact;
import idb.model.Preferences;
import idb.model.User;
import idb.model.Record;

import java.io.IOException;
import java.util.*;

public class JsonMain {
    public static void main(String[] args) {
        try {
            Database database = new Database();

            // 从配置文件加载表
            database.loadTablesFromConfig("application.properties");

            // 清空表格内容
            clearTable(database.getTable("Users"));
            clearTable(database.getTable("Addresses"));
            clearTable(database.getTable("Contacts"));
            clearTable(database.getTable("Preferences"));

            // 创建 JsonDatabaseHandler 实例，加载资源目录下的配置文件
            JsonDatabaseHandler jsonDatabaseHandler = new JsonDatabaseHandler(database, "src/main/resources/json_structure.yaml");

            // 示例 JSON 数据
            String jsonString = """
            {
                "name": "Alice",
                "age": 30,
                "address": {
                    "city": "New York",
                    "state": "NY"
                },
                "contacts": [
                    {
                        "type": "email",
                        "value": "alice@example.com"
                    },
                    {
                        "type": "phone",
                        "value": "123-456-7890"
                    }
                ],
                "preferences": {
                    "newsletter": true,
                    "notifications": {
                        "email": true,
                        "sms": false
                    }
                }
            }
            """;

            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> user = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>(){});

            // 存储 JSON 数据
            jsonDatabaseHandler.addJsonRecord(user, "User");

            // 更新 JSON 数据
            user.put("age", 31);
            Map<String, Object> address = (Map<String, Object>) user.get("address");
            address.put("city", "San Francisco");
            jsonDatabaseHandler.updateJsonRecord((int) user.get("id"), user, "User");

            // 读取 JSON 数据
            Map<String, Object> storedUser = jsonDatabaseHandler.getJsonRecord((int) user.get("id"), "User");

            // 转换回 JSON 字符串
            String storedJsonString = objectMapper.writeValueAsString(storedUser);
            System.out.println("Stored User JSON: " + storedJsonString);

        } catch (IOException | ReflectiveOperationException e) {
            e.printStackTrace();
        }
    }

    private static void clearTable(Table table) throws IOException {
        Set<Record> records = table.query(new HashMap<>());
        for (Record record : records) {
            table.deleteRecord(record.getId());
        }
    }
}
