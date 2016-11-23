import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

// - implement a (main-memory) data store with OMVCC.
// - objects are <int, int> key-value pairs.
// - if an operation is to be refused by the OMVCC protocol,
//   undo its xact (what work does this take?) and throw an exception.
// - garbage collection of versions is optional.
// - throw exceptions when necessary, such as when we try to:
//   + execute an operation in a transaction that is not running
//   + read a non-existing key
//   + delete a non-existing key
//   + write into a key where it already has an uncommitted version
// - you may but do not need to create different exceptions for operations that
//   are refused and for operations that are refused and cause the Xact to be
//   aborted. Keep it simple!
// - keep the interface, we want to test automatically!

/** ***************************************
  *         DAT Database Systems
  *              Assignment 3
  *        Cyriaque Brousse 227209
  * ***************************************/

public class OMVCC {

  /* ***************************************
                DATA STRUCTURES
   * ***************************************/
  /** Our own data store */
  private static final Map<Integer, List<Version>> store = new HashMap<>();

  /** List of active transactions */
  private static final Map<Long, Transaction> activeXacts = new HashMap<>();

  /** List of already committed transactions */
  private static final Map<Long, Transaction> committedXacts = new HashMap<>();

  private static long startAndCommitTimestampGen = 0;
  private static long transactionIdGen = 1L << 62;

  /* ***************************************
                  CC METHODS
   * ***************************************/

  /**
   * @return transaction id == logical start timestamp
   */
  public static long begin() {
    ++startAndCommitTimestampGen; //SHOULD BE USED
    ++transactionIdGen;

    final Transaction t = new Transaction(transactionIdGen, startAndCommitTimestampGen);
    activeXacts.put(transactionIdGen, t);

    return transactionIdGen;
  }

  /**
   * @return value of object key in transaction xact
   */
  public static int read(long xact, int key) throws Exception {
    final Transaction t = resolveActiveTransaction(xact);
    final List<Version> versions = store.get(key);
    if (versions == null || versions.isEmpty()) {
      throw new Exception("no value found for key " + key);
    }

    // case value already written by xact
    if (t.undoBuffer.containsKey(key)) {
      t.addReadPred(key);
      return t.undoBuffer.get(key).value;
    }

    // case iteration: return most recently committed version, before xact started
    else {
      Version lastCommitted = null;
      for (Version v : versions) {
        if (v.isCommitted() && v.timestamp < t.startTimestamp) {
          lastCommitted = v;
        }
      }

      if (lastCommitted == null) {
        throw new Exception("could not find a value for key " + key);
      } else {
        t.addReadPred(key);
        return lastCommitted.value;
      }
    }
  }

  /**
   * @return the list of values of objects whose values mod k are zero.
   * This is our only kind of query / bulk read.
   */
  public static List<Integer> modquery(long xact, int k) throws Exception {
    if (k == 0) {
      rollback(xact);
      throw new Exception("division by 0 in " + xact);
    }

    final Transaction t = resolveActiveTransaction(xact);
    final List<Integer> res = new ArrayList<>();

    for (Integer key : store.keySet()) {
      final List<Version> versions = store.get(key);
      if (versions == null || versions.isEmpty()) {
        continue;
      }

      // case value already written by xact
      if (t.undoBuffer.containsKey(key) && t.undoBuffer.get(key).value % k == 0) {
        t.addModPred(k);
        res.add(t.undoBuffer.get(key).value);
      }

      // case iteration
      else {
        Version lastCommitted = null;
        for (Version v : versions) {
          if (v.isCommitted() && v.timestamp < t.startTimestamp) {
            lastCommitted = v;
          }
        }

        if (lastCommitted != null && lastCommitted.value % k == 0) {
          t.addModPred(k);
          res.add(lastCommitted.value);
        }
      }
    }

    return res;
  }

  /**
   * update the value of an existing object identified by key
   * or insert <key,value> for a non-existing key in transaction xact
   *
   * fail if there exists already an uncommitted version or a committed version for which its
   * commitTimestamp is newer than the startTS of the current xact
   */
  public static void write(long xact, int key, int value) throws Exception {
    final Transaction t = resolveActiveTransaction(xact);
    if (store.get(key) == null) {
      store.put(key, new LinkedList<>());
    }
    final List<Version> versions = store.get(key);

    // case newer data
    if (!versions.isEmpty()
       && versions.get(versions.size() - 1).isCommitted()
       && versions.get(versions.size() - 1).timestamp > t.startTimestamp) {
      throw new Exception("newer version of data was present for key " + key);
    }

    // case insert
    else if (versions.isEmpty() || versions.get(versions.size() - 1).isCommitted()) {
      final Version v = new Version(t.id, value);
      versions.add(v);
      t.undoBuffer.put(key, v);
    }

    // case overwrite
    else if (!versions.get(versions.size() - 1).isCommitted() && t.undoBuffer.containsKey(key)) {
      final Version v = new Version(t.id, value);
      versions.set(versions.size() - 1, v);
      t.undoBuffer.put(key, v);
    }

    // case uncommitted data from other xact
    else {
      rollback(xact);
      throw new Exception("uncommitted data was already present for key " + key);
    }
  }

  /**
   * delete the object identified by key in transaction xact
   * (OPTIONAL)
   */
  public static void delete(long xact, int key) throws Exception {
    throw new UnsupportedOperationException("delete() not supported");
  }

  public static void commit(long xact) throws Exception {
    final Transaction t = resolveActiveTransaction(xact);

    if (t.validate()) {
      try {
        ++startAndCommitTimestampGen; // SHOULD BE USED
        t.doCommit(startAndCommitTimestampGen);
      } catch (Exception e) {
        rollback(xact);
        throw e;
      }
    } else {
      rollback(xact);
      throw new Exception("transaction " + t.id + " failed validation phase");
    }
  }

  public static void rollback(long xact) throws Exception {
    final Transaction t = resolveActiveTransaction(xact);

    // erase each write action performed by t
    for (Entry<Integer, Version> action : t.undoBuffer.entrySet()) {
      store.get(action.getKey()).remove(action.getValue());
    }

    activeXacts.remove(t.id);
  }

  /* ***************************************
                 CUSTOM CLASSES
   * ***************************************/

  private static final class Version {
    public long timestamp;
    public final int value;
    private boolean isCommitted = false;

    public Version(long timestamp, int value) {
      this.timestamp = timestamp;
      this.value = value;
    }

    public void commit(long timestamp) throws Exception {
      if (isCommitted) {
        throw new Exception("version " + value + " had already committed at timestamp " + timestamp);
      }

      this.timestamp = timestamp;
      this.isCommitted = true;
    }

    public boolean isCommitted() {
      return isCommitted;
    }
  }

  private static final class Transaction {
    public long id;
    public final long startTimestamp;
    private boolean hasCommitted = false;

    public Set<Integer> readPredicates = new HashSet<>();
    public Set<Integer> modPredicates  = new HashSet<>();
    public Map<Integer, Version> undoBuffer = new HashMap<>();

    public Transaction(long id, long startTimestamp) {
      this.id = id;
      this.startTimestamp = startTimestamp;
    }

    public void addReadPred(int key) {
      readPredicates.add(key);
    }

    public void addModPred(int key) {
      modPredicates.add(key);
    }

    public boolean validate() throws Exception {
      // read-only transactions are always valid
      if (undoBuffer.isEmpty()) {
        return true;
      }

      // write-only transactions are always valid
      if (readPredicates.isEmpty() && modPredicates.isEmpty()) {
        return true;
      }

      // gather all transactions that committed after this one started
      final List<Transaction> xactsCommittedAfterStart = new LinkedList<>();
      for (Entry<Long, Transaction> e : committedXacts.entrySet()) {
        if (e.getValue().id > this.startTimestamp) {
          xactsCommittedAfterStart.add(e.getValue());
        }
      }

      // check for conflicts with each transaction
      for (Transaction t : xactsCommittedAfterStart) {
        for (Entry<Integer, Version> action : t.undoBuffer.entrySet()) {
          final int key = action.getKey();
          final Version version = action.getValue();

          // WW conflict
          if (readPredicates.contains(key)) {
            return false;
          }

          // mod conflict
          if (!modPredicates.isEmpty()) {
            final Version prev = getPrevVersion(key, version);
            if (prev == null) {
              // this version is the first one for this key
              continue;
            }

            for (int k : modPredicates) {
              if (version.value % k == 0 || prev.value % k == 0) {
                return false;
              }
            }
          }
        }
      }

      return true;
    }

    public void doCommit(long timestamp) throws Exception {
      if (hasCommitted) {
        throw new Exception("transaction " + id + " has already committed at timestamp " + id);
      }

      final long oldId = this.id;

      // commit all versions written by this transaction
      for (Entry<Integer, Version> action : undoBuffer.entrySet()) {
        action.getValue().commit(timestamp);
      }

      this.hasCommitted = true;
      this.id = timestamp;

      // move to committed transactions list
      activeXacts.remove(oldId);
      committedXacts.put(id, this);
    }
  }

  /* ***************************************
                PRIVATE METHODS
   * ***************************************/

  private static Transaction resolveActiveTransaction(long xact) throws Exception {
    Transaction t = activeXacts.get(xact);

    if (t == null) {
      throw new Exception("transaction with id " + xact + " does not exist, has not started or has committed");
    }

    return t;
  }

  private static Version getPrevVersion(int key, Version version) {
    final List<Version> versions = store.get(key);
    final int indexOf = versions.indexOf(version);

    if (indexOf < 1) {
      return null;
    }

    return versions.get(indexOf - 1);
  }
}
