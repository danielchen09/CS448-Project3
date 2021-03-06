package simpledb.tx.concurrency;

import simpledb.tx.Transaction;

import java.util.*;
import java.util.stream.Collectors;

public class LockTableGraph extends LockTable {
    static class WaitForGraph {
        HashMap<Transaction, Transaction> adjList = new HashMap<>();
        Transaction head;

        void waitFor(Transaction waiting, Transaction holding) {
            if (head == null || holding == head)
                head = waiting;
            adjList.put(waiting, holding); // can only wait for 1 data item
        }

        private Transaction cycleMember() {
            // Floyd cycle detection
            Transaction slow = head;
            Transaction fast = head;
            while (slow != null && adjList.containsKey(fast)) {
                slow = adjList.get(slow);
                fast = adjList.get(adjList.get(fast));
                if (slow.equals(fast))
                    return slow;
            }
            return null;
        }

        List<Transaction> detectCycle() {
            Transaction t = cycleMember();
            if (t == null)
                return null;
            Transaction original = t;
            List<Transaction> cycle = new ArrayList<>();
            do {
                cycle.add(t);
                t = adjList.get(t);
            } while (!t.equals(original));
            return cycle;
        }
    }

    private WaitForGraph waitForGraph = new WaitForGraph();

    @Override
    void initialize(Object... dataItems) {

    }

    @Override
    synchronized void handleIncompatible(Transaction waiting, Transaction holding, LockEntry entry) throws InterruptedException {
        waitForGraph.waitFor(waiting, holding);
        List<Transaction> cycle = waitForGraph.detectCycle();
        if (cycle != null) {
            // abort the youngest transaction that has not been aborted yet
            // if all of the transactions in the cycle has been rolled back, choose the youngest one
            Transaction victim;
            List<Transaction> notAborted = cycle.stream().filter(t -> t.rollbackTime < 0).collect(Collectors.toList());
            if (notAborted.size() == 0)
                victim = Collections.max(cycle, Comparator.comparingLong(o -> o.getTimestamp()));
            else
                victim = Collections.max(notAborted, Comparator.comparingLong(o -> o.getTimestamp()));
            victim.abort();
            return;
        }
        wait();
    }

    @Override
    synchronized void handleUnlock(Transaction transaction) {
        Iterator<Map.Entry<Transaction, Transaction>> it = waitForGraph.adjList.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Transaction, Transaction> entry = it.next();
            Transaction key = entry.getKey();
            Transaction val = entry.getValue();
            if (key.equals(transaction) || val.equals(transaction)) {
                it.remove();
            }
        }
    }
}
