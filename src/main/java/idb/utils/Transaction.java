package idb.utils;

import java.util.ArrayList;
import java.util.List;

public class Transaction {
    private List<Runnable> operations;
    private boolean committed;

    public Transaction() {
        this.operations = new ArrayList<>();
        this.committed = false;
    }

    public void addOperation(Runnable operation) {
        operations.add(operation);
    }

    public void commit() {
        if (committed) {
            throw new IllegalStateException("Transaction already committed");
        }
        for (Runnable operation : operations) {
            operation.run();
        }
        committed = true;
    }

    public void rollback() {
        operations.clear();
        committed = false;
    }

    public boolean isCommitted() {
        return committed;
    }
}
