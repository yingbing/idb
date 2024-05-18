package idb;




import idb.core.Database;
import idb.core.Table;
import idb.model.Record;
import idb.utils.Transaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Main {
    public static void main(String[] args) {
        try {
            Database database = new Database();

            // 从配置文件加载表
            database.loadTablesFromConfig("application.properties");

            // 获取表
            Table usersTable = database.getTable("Users");

            // 清空表格内容
            clearTable(usersTable);

            // 添加用户记录
            Record user1 = new Record(1);
            user1.setData("name", "Alice");
            user1.setData("age", 30);
            user1.setData("country", "USA");
            usersTable.addRecord(user1);

            Record user2 = new Record(2);
            user2.setData("name", "Bob");
            user2.setData("age", 25);
            user2.setData("country", "USA");
            usersTable.addRecord(user2);

            Record user3 = new Record(3);
            user3.setData("name", "Charlie");
            user3.setData("age", 30);
            user3.setData("country", "UK");
            usersTable.addRecord(user3);

            // 创建索引
            usersTable.createIndex("age");

            // 复杂查询
            Map<String, Object> conditions = new HashMap<>();
            conditions.put("age", 30);

            System.out.println("查询结果:");
            Set<Record> results = usersTable.query(conditions);
            for (Record record : results) {
                System.out.println(record);
            }

            // 范围查询
            System.out.println("范围查询结果:");
            Set<Record> rangeResults = usersTable.rangeQuery("age", 25, 30);
            for (Record record : rangeResults) {
                System.out.println(record);
            }

            // 排序查询
            System.out.println("排序查询结果:");
            List<Record> sortedResults = usersTable.sortedQuery(conditions, "name", true);
            for (Record record : sortedResults) {
                System.out.println(record);
            }

            // 分页查询
            System.out.println("分页查询结果:");
            List<Record> paginatedResults = usersTable.paginatedQuery(conditions, 1, 2);
            for (Record record : paginatedResults) {
                System.out.println(record);
            }

            // 事务操作
            System.out.println("事务操作:");
            Transaction transaction = database.beginTransaction();
            usersTable.updateRecord(1, "age", 31, transaction);
            usersTable.deleteRecord(2, transaction);
            transaction.commit();

            System.out.println("表格状态:");
            System.out.println(usersTable);

        } catch (IOException e) {
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
