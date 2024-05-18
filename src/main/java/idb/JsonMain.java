package idb;

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
            Map<String, Object> user = new HashMap<>();
            user.put("name", "Alice");
            user.put("age", 30);

            Map<String, Object> address = new HashMap<>();
            address.put("city", "New York");
            address.put("state", "NY");
            user.put("address", address);

            Map<String, Object> contact1 = new HashMap<>();
            contact1.put("type", "email");
            contact1.put("value", "alice@example.com");

            Map<String, Object> contact2 = new HashMap<>();
            contact2.put("type", "phone");
            contact2.put("value", "123-456-7890");

            user.put("contacts", List.of(contact1, contact2));

            Map<String, Object> preferences = new HashMap<>();
            preferences.put("newsletter", true);
            preferences.put("notifications", Map.of("email", true, "sms", false));
            user.put("preferences", preferences);

            // 存储 JSON 数据
            jsonDatabaseHandler.addJsonRecord(user, "User");

            // 读取 JSON 数据
            Map<String, Object> storedUser = jsonDatabaseHandler.getJsonRecord((int) user.get("id"), "User");
            System.out.println("Stored User: " + storedUser);

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
