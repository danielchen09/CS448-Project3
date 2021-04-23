package simpledb.tx.concurrency;

import java.util.*;
import java.util.concurrent.locks.Lock;

import simpledb.file.BlockId;
import simpledb.tx.Transaction;

public abstract class LockTable {
   public static boolean VERBOSE = false;

   public enum LockTableType {
      TIMEOUT,
      WAIT_DIE,
      WOUND_WAIT,
      GRAPH
   };
   enum LockType {
      UNLOCKED,
      SHARED,
      EXCLUSIVE
   }
   class LockEntry {
      Transaction transaction;
      LockType lockType;
      boolean granted;

      public LockEntry(Transaction transaction, LockType lockType, boolean granted) {
         this.transaction = transaction;
         this.lockType = lockType;
         this.granted = granted;
      }

      @Override
      public String toString() {
         return "LockEntry{" +
                 "transaction=" + transaction +
                 ", lockType=" + lockType +
                 ", granted=" + granted +
                 '}';
      }
   }

   public static LockTable getLockTable(LockTableType type) {
      switch (type) {
         case TIMEOUT: {
            return new LockTableTimeout();
         }
         case WAIT_DIE: {
            return new LockTableWaitDie();
         }
         case WOUND_WAIT: {
            return new LockTableWoundWait();
         }
         case GRAPH: {
            return new LockTableGraph();
         }
      }
      return new LockTableTimeout();
   }

   protected Map<BlockId,LinkedList<LockEntry>> locks = new HashMap<>();

   // initialize data items, used for tree protocol
   abstract void initialize(Object... dataItems);

   /**
    * Grant an SLock on the specified block.
    * If an XLock exists when the method is called,
    * then the calling thread will be placed on a wait list
    * until the lock is released.
    * If the thread remains on the wait list for a certain 
    * amount of time (currently 10 seconds),
    * then an exception is thrown.
    * @param blk a reference to the disk block
    */
   public synchronized void sLock(Transaction transaction, BlockId blk) {
      if (VERBOSE)
         System.out.println(transaction.txnum + " request lock-S on " + blk);
      if (!locks.containsKey(blk)) {
         locks.put(blk, new LinkedList<>());
      }
      LockEntry holding = currentlyHolding(blk);

      LockEntry entry = new LockEntry(transaction, LockType.SHARED, compatible(holding, LockType.SHARED));
      locks.get(blk).add(entry);

      if (!entry.granted) {
         try {
            handleIncompatible(transaction, holding.transaction, entry);
         } catch (InterruptedException e) {
            throw new LockAbortException();
         }
      }
      if (VERBOSE)
         System.out.println(transaction.txnum + " got lock-S on " + blk);
   }

   /**
    * Grant an XLock on the specified block.
    * If a lock of any type exists when the method is called,
    * then the calling thread will be placed on a wait list
    * until the locks are released.
    * If the thread remains on the wait list for a certain
    * amount of time (currently 10 seconds),
    * then an exception is thrown.
    * @param blk a reference to the disk block
    */
   synchronized void xLock(Transaction transaction, BlockId blk) {
      if (VERBOSE)
         System.out.println(transaction.txnum + " request lock-X on " + blk);
      if (!locks.containsKey(blk)) {
         locks.put(blk, new LinkedList<>());
      }
      LockEntry holding = currentlyHolding(blk);

      LockEntry entry = new LockEntry(transaction, LockType.EXCLUSIVE, compatible(holding, LockType.EXCLUSIVE));
      locks.get(blk).add(entry);

      if (!entry.granted) {
         try {
            if (holding.transaction.equals(transaction) && holding.lockType == LockType.SHARED) {
               upgrade(blk, transaction);
            } else {
               handleIncompatible(transaction, holding.transaction, entry);
            }
         } catch (InterruptedException e) {
            throw new LockAbortException();
         }
      }
      if (VERBOSE)
         System.out.println(transaction.txnum + " got lock-X on " + blk);
   }

   // when lock cannot be granted, this function is called
   abstract void handleIncompatible(Transaction waiting, Transaction holding, LockEntry entry) throws InterruptedException;

   /**
    * Release a lock on the specified block.
    * If this lock is the last lock on that block,
    * then the waiting transactions are notified.
    * @param blk a reference to the disk block
    */
   synchronized void unlock(Transaction transaction, BlockId blk) {
      if (VERBOSE)
         System.out.println(transaction.txnum + " unlock " + blk + " check exist: " + locks.get(blk));
      handleUnlock(transaction);
      if (locks == null || locks.get(blk) == null)
         return;
      Iterator<LockEntry> it = locks.get(blk).iterator();
      while (it.hasNext()) {
         LockEntry entry = it.next();
         if (entry.transaction.equals(transaction)) {
            it.remove(); // prevent ConcurrentModificationException
            if (locks.get(blk).size() == 0) {
               // no other transaction waiting
               locks.remove(blk);
               return;
            }
            if (lockedIn(blk) == LockType.UNLOCKED) {
               // all lock entries are waiting, grant lock to first in queue
               LockEntry first = locks.get(blk).peekFirst();
               if (first.lockType == LockType.SHARED) {
                  Iterator<LockEntry> it2 = locks.get(blk).iterator();
                  while (it2.hasNext()) {
                     LockEntry entry2 = it2.next();
                     if (entry2.lockType == LockType.SHARED)
                        entry2.granted = true;
                  }
               } else {
                  first.granted = true;
               }
               notifyAll();
            }
         }
      }
   }

   synchronized public void upgrade(BlockId blk, Transaction transaction) {
      for (LockEntry entry : locks.get(blk)) {
         if (entry.transaction.equals(transaction) && entry.lockType == LockType.SHARED) {
            entry.lockType = LockType.EXCLUSIVE;
            return;
         }
      }
   }

   abstract void handleUnlock(Transaction transaction);

   synchronized public LockType lockedIn(BlockId blk) {
      if (!locks.containsKey(blk))
         return LockType.UNLOCKED;
      for (LockEntry entry : locks.get(blk)) {
         if (entry.granted)
            return entry.lockType;
      }
      return LockType.UNLOCKED;
   }

   synchronized public LockEntry currentlyHolding(BlockId blk) {
      for (LockEntry entry : locks.get(blk)) {
         if (entry.granted)
            return entry;
      }
      return null;
   }

   // implements the compatibility matrix of S and X
   synchronized public boolean compatible(LockEntry current, LockType type) {
      if (current == null)
         return true;
      if (current.lockType == LockType.UNLOCKED)
         return  true;
      if (current.lockType == LockType.SHARED && type == LockType.SHARED)
         return true;
      return false;
   }


}
