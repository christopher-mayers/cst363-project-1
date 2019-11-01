package disk_store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
	
	/* 
	 * List to hold index numbers and associated block numbers
	 * Example List : [[1,1,2], [2,1,2,3], [3,1], [4,3]]
	 * List explanation: Each inner list starts with the search key value, 
	 *					 and the following numbers at the block numbers 
	 *  				 associated with that search key value
	 */                                   
	
	private List <List <Integer>> index; 
	
	// Constructor initializes list as empty list
	public OrdIndex() {
		
		index = new ArrayList <List <Integer>>(); // Initialize index as a map
	
	}
	
	/* 
	 * Modified binary search to return position to add a new index.
	 * Usually, a binary search returns a mark value if the value
	 * being searched for is not in the array. However, this search has
	 * been modified to return the position that the search value would be 
	 * inserted if it is not found within the array. This search examines 
	 * the first value of each sub-array, as this is the search key value.
	 */
	int addSearch(List <List <Integer>> arr, int l, int r, int x) 
    { 
		int mid = l + (r - l) / 2;
		
        if (r >= l) 
        { 

            if (arr.get(mid).get(0) == x) 
            {
            	return mid; 
        	}
        
            if (arr.get(mid).get(0) > x)
            {
            	return addSearch(arr, l, mid - 1, x); 
            } 

            return addSearch(arr, mid + 1, r, x); 
        } 
        return mid; 
    } 
	
	/*
	 * Regular binary search to determine position of search key within array.
	 * Searches first values of each sub-array.
	 */
	int binarySearch(List <List <Integer>> arr, int l, int r, int x) 
    { 
		
        if (r >= l) 
        { 

        	int mid = l + (r - l) / 2;
        	
            if (arr.get(mid).get(0) == x) 
            {
            	return mid; 
        	}
        
            if (arr.get(mid).get(0) > x)
            {
            	return binarySearch(arr, l, mid - 1, x); 
            } 

            return binarySearch(arr, mid + 1, r, x); 
        } 
        return -1; 
    } 
	
	/*
	 * Binary search designed to find block number located within
	 * sub-array. Doesn't search through full index, but a given
	 * sub-array with block number values within.
	 */
	int deleteSearch(List <Integer> arr, int l, int r, int x) 
    { 
		
        if (r >= l) 
        { 

        	int mid = l + (r - l) / 2;
        	
            if (arr.get(mid) == x) 
            {
            	return mid; 
        	}
        
            if (arr.get(mid) > x)
            {
            	return deleteSearch(arr, l, mid - 1, x); 
            } 

            return deleteSearch(arr, mid + 1, r, x); 
        } 
        return -1; 
    } 
	
	// Function to locate a search key within an index and return block numbers at that index
	@Override
	public List<Integer> lookup(int key) {
		
		// Get location of search key within index using binary search function
		// Search from beginning to end of index
		int ind = binarySearch(index, 0, index.size()-1, key);
		List HoldList = new ArrayList <Integer>();
		
		// If the search key does not exist, return an empty list
		if (ind == -1)
		{
			return HoldList;
		}
		
		// If search key does exist, generate a sub list of all block numbers 
		// associated with that search key
		HoldList = index.get(ind).subList(1, index.get(ind).size());
		
		// Because the index can store repeat block numbers, but the returned
		// list should not store repeat block numbers, create a set of the
		// unique values within the sub-array associated with the search key
		Set <Integer> blockSet = new HashSet <Integer>();
		
		// Iterate through list with values from sub-array and add to set
		for (int i = 0; i < HoldList.size(); i++)
		{
			blockSet.add((Integer) HoldList.get(i));
		}
		
		// Add set values to a new list to match expected return type
		List HoldList2 = new ArrayList <Integer>();
		
		for (Integer b : blockSet)
		{
			HoldList2.add(b);
		}
		
		return HoldList2;
	}
	
	// Function to insert a key and block number within an index
	@Override
	public void insert(int key, int blockNum) {
		//System.out.println(index);
		// If index is not empty
		if (index.size() > 0)
		{
			// Use binary search to determine position of key and block number insert
			int ind = addSearch(index, 0, index.size()-1, key);
			
			// Determine if key goes at end of index
			if (ind > index.size()-1)
			{
				// Create new list and add to index at discovered location
				ArrayList holdList = new ArrayList <Integer>();
				index.add(holdList);
				// Add key and block number to list at index at discovered location
				index.get(ind).add(key);
				index.get(ind).add(blockNum);
			}
			else
			{
				// Determine if key goes at position already occupied by different key value
				if (index.get(ind).get(0) != key)
				{
					// Create new list and add to index at discovered location
					ArrayList holdList = new ArrayList <Integer>();
					index.add(ind, holdList);
					// Add key and block number to list at index at discovered location
					index.get(ind).add(key);
					index.get(ind).add(blockNum);
				}
				// Determine if key already exists in index
				else
				{
					// Add block number to already existing list
					index.get(ind).add(blockNum);
					// Sort values in index at search key minus the search key value
					Collections.sort(index.get(ind).subList(1, index.get(ind).size()));
				}
			}
		}
		// If index is empty
		else
		{
			// Create new array and add to index
			ArrayList holdList = new ArrayList <Integer>();
			index.add(0, holdList);
			// Insert key and block number to the list in index
			index.get(0).add(key);
			index.get(0).add(blockNum);
		}
	}

	@Override
	public void delete(int key, int blockNum) {
		
		// Determine if key exists in index with binary search
		int ind = binarySearch(index, 0, index.size()-1, key);
		
		// Determine if key exists in the index
		if (ind != -1)
		{
			// Get list of all block numbers associated with search key
			List holdList =  index.get(ind).subList(1, index.get(ind).size());
			
			// Run block number list through binary search to find index of block number
			int ind2 = deleteSearch(holdList, 0, holdList.size()-1, blockNum);
			
			// Delete block number if it exists in list
			if (ind2 != -1)
			{
				index.get(ind).remove(ind2+1);
			}
			
			// If index at key becomes size 1, this means that only the search key
			// value is left, and therefore can be safely deleted from the index.
			if (index.get(ind).size() == 1)
			{
				index.remove(ind);
			}
		}
	}
	
	/**
	 * Return the number of entries in the index
	 * @return
	 */
	public int size() {
		int size = 0;
		
		// Count size as number of values in each sub-array of index minus 1 for
		// each sub-array as the first value is the search key, which shouldn't
		// be counted.
		for (int i = 0; i < index.size(); i++)
		{
			size += index.get(i).size()-1;
		}
		return size;
	}
	
	@Override
	public String toString() {
		throw new UnsupportedOperationException();
	}
}
