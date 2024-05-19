package idb.core;

import idb.model.Record;
import idb.utils.Cache;
import idb.utils.Transaction;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class TableImpl implements Table {
    private static final Logger logger = Logger.getLogger(TableImpl.class.getName());
    private String name;
    private Map<Integer, Record> records;
    private Cache<Integer, Record> cache;
    private String csvFilePath;
    private Map<String, Map<Object, Set<Integer>>> singleColumnIndexes;
    private Map<String, Map<List<Object>, Set<Integer>>> multiColumnIndexes;
    private Set<String> uniqueColumns;
    private ReentrantReadWriteLock lock;

    public TableImpl(String name, String csvFilePath) {
        this.name = name;
        this.csvFilePath = csvFilePath;
        this.records = new HashMap<>();
        this.cache = new Cache<>(100); // 设置缓存大小
        this.singleColumnIndexes = new HashMap<>();
        this.multiColumnIndexes = new HashMap<>();
        this.uniqueColumns = new HashSet<>();
        this.lock = new ReentrantReadWriteLock();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void addUniqueConstraint(String columnName) {
        uniqueColumns.add(columnName);
    }

    private boolean isUniqueConstraintViolated(String columnName, Object value) {
        for (Record record : records.values()) {
            if (record.getData(columnName).equals(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void addRecord(Record record) throws IOException {
        addRecord(record, null);
    }

    @Override
    public void addRecord(Record record, Transaction transaction) throws IOException {
        if (transaction != null) {
            transaction.addOperation(() -> {
                try {
                    addRecord(record, null);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            return;
        }

        lock.writeLock().lock();
        try {
            for (String column : uniqueColumns) {
                if (isUniqueConstraintViolated(column, record.getData(column))) {
                    throw new IOException("Unique constraint violated for column: " + column);
                }
            }
            records.put(record.getId(), record);
            cache.put(record.getId(), record); // 添加到缓存
            updateIndexes(record);
            saveToCSV();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Record getRecord(int id) {
        lock.readLock().lock();
        try {
            Record record = cache.get(id);
            if (record == null) {
                record = records.get(id);
                if (record != null) {
                    cache.put(id, record); // 添加到缓存
                }
            }
            return record;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void updateRecord(int id, String columnName, Object newValue) throws IOException {
        updateRecord(id, columnName, newValue, null);
    }

    @Override
    public void updateRecord(int id, String columnName, Object newValue, Transaction transaction) throws IOException {
        if (transaction != null) {
            transaction.addOperation(() -> {
                try {
                    updateRecord(id, columnName, newValue, null);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            return;
        }

        lock.writeLock().lock();
        try {
            Record record = records.get(id);
            if (record != null) {
                record.setData(columnName, newValue);
                cache.put(id, record); // 更新缓存
                updateIndexes(record);
                saveToCSV();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void updateRecord(Record record) throws IOException {
        lock.writeLock().lock();
        try {
            records.put(record.getId(), record);
            cache.put(record.getId(), record); // 更新缓存
            updateIndexes(record);
            saveToCSV();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void deleteRecord(int id) throws IOException {
        deleteRecord(id, null);
    }

    @Override
    public void deleteRecord(int id, Transaction transaction) throws IOException {
        if (transaction != null) {
            transaction.addOperation(() -> {
                try {
                    deleteRecord(id, null);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            return;
        }

        lock.writeLock().lock();
        try {
            Record record = records.remove(id);
            if (record != null) {
                cache.remove(id); // 从缓存中移除
                removeIndexes(record);
                saveToCSV();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void createIndex(String columnName) {
        lock.writeLock().lock();
        try {
            Map<Object, Set<Integer>> index = new HashMap<>();
            for (Record record : records.values()) {
                Object key = record.getData(columnName);
                index.computeIfAbsent(key, k -> new HashSet<>()).add(record.getId());
            }
            singleColumnIndexes.put(columnName, index);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Record getRecordByIndex(String columnName, Object value) {
        lock.readLock().lock();
        try {
            Map<Object, Set<Integer>> index = singleColumnIndexes.get(columnName);
            if (index != null) {
                Set<Integer> ids = index.get(value);
                if (ids != null && !ids.isEmpty()) {
                    return getRecord(ids.iterator().next()); // 返回第一个匹配的记录
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Set<Record> getRecordsByMultiColumnIndex(String[] columnNames, Object[] values) {
        lock.readLock().lock();
        try {
            String indexName = String.join("_", columnNames);
            Map<List<Object>, Set<Integer>> index = multiColumnIndexes.get(indexName);
            if (index != null) {
                List<Object> key = Arrays.asList(values);
                Set<Integer> ids = index.get(key);
                if (ids != null && !ids.isEmpty()) {
                    Set<Record> recordsSet = new HashSet<>();
                    for (Integer id : ids) {
                        recordsSet.add(getRecord(id));
                    }
                    return recordsSet;
                }
            }
            return Collections.emptySet();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Map<Integer, Record> queryRecords(String columnName, Object value) {
        lock.readLock().lock();
        try {
            Map<Integer, Record> result = new HashMap<>();
            for (Record record : records.values()) {
                if (record.getData(columnName).equals(value)) {
                    result.put(record.getId(), record);
                }
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Set<Record> query(Map<String, Object> conditions) {
        lock.readLock().lock();
        try {
            Set<Integer> resultIds = null;

            // 使用单列索引进行查询
            for (String column : conditions.keySet()) {
                if (singleColumnIndexes.containsKey(column)) {
                    Map<Object, Set<Integer>> index = singleColumnIndexes.get(column);
                    Object value = conditions.get(column);
                    if (index.containsKey(value)) {
                        Set<Integer> ids = index.get(value);
                        if (resultIds == null) {
                            resultIds = new HashSet<>(ids);
                        } else {
                            resultIds.retainAll(ids);
                        }
                    } else {
                        return Collections.emptySet(); // 没有匹配的记录
                    }
                }
            }

            // 使用多列索引进行查询
            for (String indexName : multiColumnIndexes.keySet()) {
                String[] columns = indexName.split("_");
                List<Object> key = new ArrayList<>();
                boolean allColumnsMatch = true;
                for (String column : columns) {
                    if (conditions.containsKey(column)) {
                        key.add(conditions.get(column));
                    } else {
                        allColumnsMatch = false;
                        break;
                    }
                }

                if (allColumnsMatch) {
                    Map<List<Object>, Set<Integer>> index = multiColumnIndexes.get(indexName);
                    if (index.containsKey(key)) {
                        Set<Integer> ids = index.get(key);
                        if (resultIds == null) {
                            resultIds = new HashSet<>(ids);
                        } else {
                            resultIds.retainAll(ids);
                        }
                    } else {
                        return Collections.emptySet(); // 没有匹配的记录
                    }
                }
            }

            // 如果没有使用索引，则遍历所有记录
            if (resultIds == null) {
                resultIds = new HashSet<>();
                for (Record record : records.values()) {
                    boolean match = true;
                    for (String column : conditions.keySet()) {
                        if (!record.getData(column).equals(conditions.get(column))) {
                            match = false;
                            break;
                        }
                    }
                    if (match) {
                        resultIds.add(record.getId());
                    }
                }
            }

            // 从记录中获取匹配的结果集
            Set<Record> resultSet = new HashSet<>();
            for (Integer id : resultIds) {
                resultSet.add(getRecord(id));
            }
            return resultSet;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Set<Record> rangeQuery(String column, Object start, Object end) {
        lock.readLock().lock();
        try {
            Set<Record> resultSet = new HashSet<>();
            for (Record record : records.values()) {
                Object value = record.getData(column);
                if (value instanceof Comparable) {
                    Comparable comparableValue = (Comparable) value;
                    if ((start == null || comparableValue.compareTo(start) >= 0) &&
                            (end == null || comparableValue.compareTo(end) <= 0)) {
                        resultSet.add(record);
                    }
                }
            }
            return resultSet;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<Record> sortedQuery(Map<String, Object> conditions, String sortBy, boolean ascending) {
        Set<Record> resultSet = query(conditions);
        List<Record> resultList = new ArrayList<>(resultSet);

        resultList.sort((r1, r2) -> {
            Comparable value1 = (Comparable) r1.getData(sortBy);
            Comparable value2 = (Comparable) r2.getData(sortBy);
            return ascending ? value1.compareTo(value2) : value2.compareTo(value1);
        });

        return resultList;
    }

    @Override
    public List<Record> paginatedQuery(Map<String, Object> conditions, int page, int pageSize) {
        List<Record> resultList = new ArrayList<>(query(conditions));
        int fromIndex = (page - 1) * pageSize;
        int toIndex = Math.min(fromIndex + pageSize, resultList.size());

        if (fromIndex >= resultList.size() || fromIndex < 0) {
            return Collections.emptyList();
        }

        return resultList.subList(fromIndex, toIndex);
    }

    private void updateIndexes(Record record) {
        for (String columnName : singleColumnIndexes.keySet()) {
            Map<Object, Set<Integer>> index = singleColumnIndexes.get(columnName);
            Object key = record.getData(columnName);
            index.computeIfAbsent(key, k -> new HashSet<>()).add(record.getId());
        }
        for (String indexName : multiColumnIndexes.keySet()) {
            Map<List<Object>, Set<Integer>> index = multiColumnIndexes.get(indexName);
            List<Object> key = new ArrayList<>();
            String[] columnNames = indexName.split("_");
            for (String columnName : columnNames) {
                key.add(record.getData(columnName));
            }
            index.computeIfAbsent(key, k -> new HashSet<>()).add(record.getId());
        }
    }

    private void removeIndexes(Record record) {
        for (String columnName : singleColumnIndexes.keySet()) {
            Map<Object, Set<Integer>> index = singleColumnIndexes.get(columnName);
            Object key = record.getData(columnName);
            Set<Integer> ids = index.get(key);
            if (ids != null) {
                ids.remove(record.getId());
                if (ids.isEmpty()) {
                    index.remove(key);
                }
            }
        }
        for (String indexName : multiColumnIndexes.keySet()) {
            Map<List<Object>, Set<Integer>> index = multiColumnIndexes.get(indexName);
            List<Object> key = new ArrayList<>();
            String[] columnNames = indexName.split("_");
            for (String columnName : columnNames) {
                key.add(record.getData(columnName));
            }
            Set<Integer> ids = index.get(key);
            if (ids != null) {
                ids.remove(record.getId());
                if (ids.isEmpty()) {
                    index.remove(key);
                }
            }
        }
    }

    @Override
    public void saveToCSV() throws IOException {
        lock.writeLock().lock();
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFilePath)))) {
            writer.write("id");
            if (!records.isEmpty()) {
                for (String columnName : records.values().iterator().next().getData().keySet()) {
                    writer.write("," + columnName);
                }
            }
            writer.write("\n");
            for (Record record : records.values()) {
                writer.write(record.getId() + "");
                for (Object value : record.getData().values()) {
                    writer.write("," + value);
                }
                writer.write("\n");
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void loadFromCSV() throws IOException {
        lock.writeLock().lock();
        try {
            File file = new File(csvFilePath);
            if (!file.exists()) {
                return;
            }

            records.clear();
            cache.clear(); // 清空缓存
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
                String line = reader.readLine(); // Read header
                if (line == null) return;
                String[] headers = line.split(",");
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",");
                    int id = Integer.parseInt(parts[0]);
                    Record record = new Record(id);
                    for (int i = 1; i < headers.length; i++) {
                        record.setData(headers[i], parts[i]);
                    }
                    records.put(id, record);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        return "Table{name='" + name + "', records=" + records + "}";
    }
}
