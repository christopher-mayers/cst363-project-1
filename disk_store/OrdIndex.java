package disk_store;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

/**
 * An ordered index.  Duplicate search key values are allowed,
 * but not duplicate index table entries.  In DB terminology, a
 * search key is not a superkey.
 * 
 * A limitation of this class is that only single integer search
 * keys are supported.
 *
 */

public class OrdIndex implements DBIndex {
	
	/**
	 * Create an new ordered index.
	 */
	
	private HashMap <Integer, List> index;
	
	public OrdIndex() {
		
		index = new HashMap <Integer, List>();
		
		//throw new UnsupportedOperationException();
	}
	
	@Override
	public List<Integer> lookup(int key) {
		//throw new UnsupportedOperationException();
		
		if (index.containsKey(key))
		{
			Set <Integer> valueSet = new HashSet <Integer>();
			ArrayList <Integer> hold = (ArrayList) index.get(key);
			for (int h = 0; h < hold.size(); h++)
			{
				valueSet.add(hold.get(h));
			}
			ArrayList <Integer> returnList = new ArrayList<Integer>();
			for (int v: valueSet)
			{
				returnList.add(v);
			}
			return returnList;
		}
		else
		{
			return new ArrayList<Integer> ();
		}
	}
	
	@Override
	public void insert(int key, int blockNum) {
		
		Integer ikey = key;
		Integer iblocknum = blockNum;
		
		if (index.get(ikey) == null)
		{
			index.put(ikey, new ArrayList <Integer> ());
		}
		
		index.get(ikey).add(iblocknum);	
		
		System.out.println(index);
	}

	@Override
	public void delete(int key, int blockNum) {
		if (index.get(key).contains(blockNum))
		{
			int ind = index.get(key).indexOf(blockNum);
			index.get(key).remove(ind);
		}
		System.out.println(index);
	}
	
	/**
	 * Return the number of entries in the index
	 * @return
	 */
	public int size() {
		int size = 0;
		for (int i : index.keySet())
		{
			int hold = index.get(i).size();
			size += hold;
		}
		// you may find it useful to implement this
		return size;
	}
	
	@Override
	public String toString() {
		throw new UnsupportedOperationException();
	}
}
