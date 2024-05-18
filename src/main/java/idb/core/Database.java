package idb.core;



import idb.core.Table;
import idb.core.TableImpl;
import idb.utils.ConfigUtils;
import idb.utils.Transaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Database {
    private static final Logger logger = Logger.getLogger(Database.class.getName());
    private Map<String, Table> tables;

    public Database() {
        this.tables = new HashMap<>();
        try {
            FileHandler fileHandler = new FileHandler("database.log", true);
            fileHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(fileHandler);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loadTablesFromConfig(String configFileName) throws IOException {
        ConfigUtils config = new ConfigUtils(configFileName);
        String[] tableNames = config.getTables();
        for (String tableName : tableNames) {
            String name = config.getProperty("table." + tableName + ".name");
            String csvFilePath = config.getProperty("table." + tableName + ".csvFilePath");
            String uniqueColumns = config.getProperty("table." + tableName + ".uniqueColumns");
            if (name != null && csvFilePath != null) {
                Table table = new TableImpl(name, csvFilePath);
                if (uniqueColumns != null) {
                    for (String column : uniqueColumns.split(",")) {
                        table.addUniqueConstraint(column.trim());
                    }
                }
                table.loadFromCSV();
                tables.put(name, table);
                logger.info("Table loaded from config: " + name);
            }
        }
    }

    public synchronized void createTable(String tableName, String csvFilePath) throws IOException {
        if (tables.containsKey(tableName)) {
            throw new IOException("Table with name " + tableName + " already exists.");
        }
        Table table = new TableImpl(tableName, csvFilePath);
        table.loadFromCSV();
        tables.put(tableName, table);
        logger.info("Table created: " + tableName);
    }

    public synchronized Table getTable(String tableName) {
        return tables.get(tableName);
    }

    public synchronized void deleteTable(String tableName) throws IOException {
        Table table = tables.remove(tableName);
        if (table != null) {
            logger.info("Table deleted: " + tableName);
        }
    }

    public synchronized Map<String, Table> getTables() {
        return new HashMap<>(tables); // Return a copy to prevent modification
    }

    public synchronized void setTables(Map<String, Table> tables) {
        this.tables = new HashMap<>(tables);
    }

    public Transaction beginTransaction() {
        return new Transaction();
    }

    @Override
    public String toString() {
        return "Database{tables=" + tables + "}";
    }
}
