package idb.model;

import java.util.Map;

public class Preferences {
    private int id;
    private int parentId;
    private boolean newsletter;
    private Map<String, Boolean> notifications;

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

    public boolean isNewsletter() {
        return newsletter;
    }

    public void setNewsletter(boolean newsletter) {
        this.newsletter = newsletter;
    }

    public Map<String, Boolean> getNotifications() {
        return notifications;
    }

    public void setNotifications(Map<String, Boolean> notifications) {
        this.notifications = notifications;
    }

    @Override
    public String toString() {
        return "Preferences{" +
                "id=" + id +
                ", parentId=" + parentId +
                ", newsletter=" + newsletter +
                ", notifications=" + notifications +
                '}';
    }
}