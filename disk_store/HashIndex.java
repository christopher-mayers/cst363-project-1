package disk_store;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
	private Map<Integer, ArrayList<Integer>> index;
	
	public HashIndex() {
		index = new HashMap<Integer, ArrayList <Integer>>();
	}
	
	@Override
	public List<Integer> lookup(int key) {
		List <Integer> result = new ArrayList <Integer>();
		List <Integer> result2 = new ArrayList <Integer>();
		if (index.containsKey(key))
		{
			result = index.get(key);
			Set <Integer> set = new HashSet <Integer>();
			for (int r : result)
			{
				set.add(r);
			}
			for (int s : set)
			{
				result2.add(s);
			}
		}
		return result2;
	}
	
	@Override
	public void insert(int key, int blockNum) {
		ArrayList <Integer> hold = new ArrayList <Integer>();
		if (index.containsKey(key))
		{
			hold = index.get(key);
			hold.add(blockNum);
			Collections.sort(hold);
			index.put(key, hold);
		}
		else
		{
			hold.add(blockNum);
			index.put(key, hold);
		}
	}

	@Override
	public void delete(int key, int blockNum) {
		if (index.containsKey(key))
		{
			ArrayList <Integer> hold = new ArrayList <Integer>();
			hold = index.get(key);
			int ind = hold.indexOf(blockNum);
			if (ind != -1)
			{
				hold.remove(ind);
				if (hold.size() == 0)
				{
					index.remove(key);
				}
				else
				{
					index.put(key, hold);
				}
			}
		}
	}
	
	@Override
	public String toString() {
		String str = "";
		Set <Integer> keys = index.keySet();
		for (Integer k : keys)
		{
			str += k + ": " + index.get(k) + "\n";
		}
		return str;
	}
	

	public int size() {
		int size = 0;
		Set <Integer> keys = index.keySet();
		for (Integer k : keys)
		{
			size+=index.get(k).size();
		}
		return size;
	}
}
