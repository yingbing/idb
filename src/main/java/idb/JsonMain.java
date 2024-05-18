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
            User user = new User();
            user.setId(UUID.randomUUID().hashCode());
            user.setName("Alice");
            user.setAge(30);

            Address address = new Address();
            address.setCity("New York");
            address.setState("NY");
            user.setAddress(address);

            Contact contact1 = new Contact();
            contact1.setType("email");
            contact1.setValue("alice@example.com");

            Contact contact2 = new Contact();
            contact2.setType("phone");
            contact2.setValue("123-456-7890");

            user.setContacts(List.of(contact1, contact2));

            Preferences preferences = new Preferences();
            preferences.setNewsletter(true);
            preferences.setNotifications(Map.of("email", true, "sms", false));
            user.setPreferences(preferences);

            // 存储 JSON 数据
            jsonDatabaseHandler.addJsonRecord(user);

            // 读取 JSON 数据
            User storedUser = jsonDatabaseHandler.getJsonRecord(user.getId(), User.class);
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
