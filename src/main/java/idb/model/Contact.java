package idb.model;

public class Contact {
    private int id;
    private int parentId;
    private String type;
    private String value;

    // Getters and Setters
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getParentId() {
        return parentId;
    }

    public void setParentId(int parentId) {
        this.parentId = parentId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Contact{" +
                "id=" + id +
                ", parentId=" + parentId +
                ", type='" + type + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}