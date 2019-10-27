package disk_store;

import java.util.List;

/**
 * A high-level DB interface for a relational database in which
 * every table must have a key
 * @author Glenn
 *
 */

public interface DB {
	/**
	 * Insert the given record, return true on success.
	 * @param rec
	 * @return
	 */
	boolean insert(Record rec);
	
	/**
	 * Delete the record with the given key, return true on success.
	 * @param key
	 * @return
	 */
	boolean delete(int key);	
	
	/**
	 * Modify the record with the key of the given record, replacing field
	 * values with values of the given record.  Return true on success.
	 * @param rec
	 * @return
	 */
	boolean modify(Record rec);     // modify the record, return true on success
	
	/**
	 * Return the record with the given primary key, else return null if no
	 * such record.
	 * @param key
	 * @return
	 */
	Record lookup(int key);

	/**
	 * Return all records in which the given field has the given search key value.
	 * @param key
	 * @return
	 */
	List<Record> lookup(String field, int key);
}
