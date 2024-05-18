package idb.model;

import java.util.HashMap;
import java.util.Map;

public class Record {
    private int id;
    private Map<String, Object> data;

    public Record(int id) {
        this.id = id;
        this.data = new HashMap<>();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Object getData(String columnName) {
        return data.get(columnName);
    }

    public void setData(String columnName, Object value) {
        data.put(columnName, value);
    }

    public Map<String, Object> getData() {
        return data;
    }

    @Override
    public String toString() {
        return "Record{id=" + id + ", data=" + data + "}";
    }
}
