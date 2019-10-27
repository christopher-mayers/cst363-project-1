package disk_store;

import java.util.List;

/**
 * A database index that associates search key values with the block
 * numbers containing records with that search key.  Note that a search
 * key is not necessarily a superkey.
 * 
 * @author Glenn
 *
 */

public interface DBIndex {

	/**
	 * Insert the key/blockNum pair into the index.  If the pair is
	 * already present, it is not inserted.
	 * @param key value of a search key
	 * @param blockNum a DB block number
	 */
	public void insert(int key, int blockNum);
	
	/**
	 * Delete the key/blockNum pair from the index.  If the pair is
	 * not present, nothing is done.
	 * @param key value of a search key
	 * @param blockNum a DB block number
	 */
	public void delete(int key, int blockNum);
	
	/**
	 * Return a list of all the blockNum values associated with the
	 * given search key in the index (return an empty list if the
	 * key does not appear in the index).
	 * @param key value of a search key
	 * @return
	 */
	public List<Integer> lookup(int key);
}
