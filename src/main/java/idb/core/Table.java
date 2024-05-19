package idb.core;

import idb.model.Record;
import idb.utils.Transaction;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Table {
    String getName();
    void addUniqueConstraint(String columnName);
    void addRecord(Record record) throws IOException;
    void addRecord(Record record, Transaction transaction) throws IOException;
    void updateRecord(int id, String columnName, Object newValue) throws IOException;
    void updateRecord(int id, String columnName, Object newValue, Transaction transaction) throws IOException;
    void updateRecord(Record record) throws IOException;
    void deleteRecord(int id) throws IOException;
    void deleteRecord(int id, Transaction transaction) throws IOException;
    Record getRecord(int id);
    void createIndex(String columnName);
    Record getRecordByIndex(String columnName, Object value);
    Set<Record> getRecordsByMultiColumnIndex(String[] columnNames, Object[] values);
    Map<Integer, Record> queryRecords(String columnName, Object value);
    Set<Record> query(Map<String, Object> conditions);
    Set<Record> rangeQuery(String column, Object start, Object end);
    List<Record> sortedQuery(Map<String, Object> conditions, String sortBy, boolean ascending);
    List<Record> paginatedQuery(Map<String, Object> conditions, int page, int pageSize);
    void saveToCSV() throws IOException;
    void loadFromCSV() throws IOException;
}
