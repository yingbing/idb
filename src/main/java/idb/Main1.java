package idb;

import idb.core.Database;
import idb.core.Table;
import idb.model.Record;

import java.io.IOException;
import java.util.Map;

public class Main1 {
    public static void main(String[] args) {
        try {
            Database db = new Database();

            // 加载 Users 表
            db.createTable("Users", "users.csv");
            Table usersTable = db.getTable("Users");

            // 加载 Products 表
            db.createTable("Products", "products.csv");
            Table productsTable = db.getTable("Products");

            // 显示加载后的记录
            System.out.println("All records in Users table: " + usersTable);
            System.out.println("All records in Products table: " + productsTable);

            // 查询 Users 表
            usersTable.createIndex("name");
            Record indexedUser = usersTable.getRecordByIndex("name", "Alice");
            System.out.println("Indexed record with name 'Alice' in Users table: " + indexedUser);

            // 使用查询接口查询 Users 表
            Map<Integer, Record> userQueryResults = usersTable.queryRecords("name", "Alice");
            System.out.println("Query results for name 'Alice' in Users table: " + userQueryResults);

            // 查询 Products 表
            productsTable.createIndex("name");
            Record indexedProduct = productsTable.getRecordByIndex("name", "Laptop");
            System.out.println("Indexed record with name 'Laptop' in Products table: " + indexedProduct);

            // 使用查询接口查询 Products 表
            Map<Integer, Record> productQueryResults = productsTable.queryRecords("name", "Laptop");
            System.out.println("Query results for name 'Laptop' in Products table: " + productQueryResults);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
