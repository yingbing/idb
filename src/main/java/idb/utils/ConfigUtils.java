package idb.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtils {
    private Properties properties;

    public ConfigUtils(String configFileName) {
        properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(configFileName)) {
            if (input == null) {
                System.out.println("Sorry, unable to find " + configFileName);
                return;
            }
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public String[] getTables() {
        String tables = properties.getProperty("database.tables");
        return tables != null ? tables.split(",") : new String[0];
    }
}
