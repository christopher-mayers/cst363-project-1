package disk_store;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A hash index.  
 * 
 */

public class HashIndex implements DBIndex {

	/**
	 * Create an new index.
	 */
	public HashIndex() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public List<Integer> lookup(int key) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public void insert(int key, int blockNum) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void delete(int key, int blockNum) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public String toString() {
		throw new UnsupportedOperationException();
	}
}
