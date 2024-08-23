package idb;

import com.fasterxml.jackson.core.type.TypeReference;
import idb.nosql.NoSQLDatabase;

import java.util.List;

public class NosqlMain {
    public static void main(String[] args) {
        NoSQLDatabase db = new NoSQLDatabase();

        // 插入数据
        db.put("user:1", "Alice");
        db.put("user:2", "Bob");

        // 使用 JSON 存储复杂对象
        db.putJson("user:3", new User("Charlie", 30));

        // 使用 TypeReference 获取复杂类型的数据
        db.putJson("users:list", List.of(new User("Dave", 25), new User("Eve", 28)));

        // 获取存储的 JSON 数据并转换为对象
        User user3 = db.getAsObject("user:3", new TypeReference<User>() {});
        System.out.println("User 3: " + user3);

        // 获取存储的列表
        List<User> users = db.getAsObject("users:list", new TypeReference<List<User>>() {});
        System.out.println("Users list: " + users);

        // 删除数据
        db.delete("user:2");

        // 查询所有数据
        System.out.println("All users: " + db.queryAll());
    }
}

// 示例用户类
class User {
    private String name;
    private int age;

    public User() {}

    public User(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "User{name='" + name + "', age=" + age + "}";
    }
}