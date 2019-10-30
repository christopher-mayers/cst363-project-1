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
	
	private List <List <Integer>> index; // Map to hold index numbers and associated block numbers
	
	public OrdIndex() {
		
		index = new ArrayList <List <Integer>>(); // Initialize index as a map
	
	}
	
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
	
	@Override
	public List<Integer> lookup(int key) {
		int ind = binarySearch(index, 0, index.size()-1, key);
		List HoldList = new ArrayList <Integer>();
		
		if (ind == -1)
		{
			return HoldList;
		}
		
		HoldList = index.get(ind).subList(1, index.get(ind).size());
		
		Set <Integer> blockSet = new HashSet <Integer>();
		
		for (int i = 0; i < HoldList.size(); i++)
		{
			blockSet.add((Integer) HoldList.get(i));
		}
		
		List HoldList2 = new ArrayList <Integer>();
		
		for (Integer b : blockSet)
		{
			HoldList2.add(b);
		}
		
		return HoldList2;
	}
	
	@Override
	public void insert(int key, int blockNum) {
		
		if (index.size() > 0)
		{
			int ind = addSearch(index, 0, index.size()-1, key);
			
			if (ind > index.size()-1)
			{
				ArrayList holdList = new ArrayList <Integer>();
				index.add(holdList);
				index.get(ind).add(key);
				index.get(ind).add(blockNum);
			}
			else
			{
				index.get(ind).add(blockNum);
				Collections.sort(index.get(ind).subList(1, index.get(ind).size()));
			}
		}
		else if (index.size() == 1)
		{
			if (key < index.get(0).get(0))
			{
				ArrayList holdList = new ArrayList <Integer>();
				index.add(0, holdList);
				index.get(0).add(key);
				index.get(0).add(blockNum);
			}
			else
			{
				ArrayList holdList = new ArrayList <Integer>();
				index.add(holdList);
				index.get(1).add(key);
				index.get(1).add(blockNum);
			}
		}
		else
		{
			ArrayList holdList = new ArrayList <Integer>();
			index.add(0, holdList);
			index.get(0).add(key);
			index.get(0).add(blockNum);
		}
	}

	@Override
	public void delete(int key, int blockNum) {
		
		int ind = binarySearch(index, 0, index.size()-1, key);
		if (ind != -1)
		{
			List holdList =  index.get(ind).subList(1, index.get(ind).size());
			int ind2 = deleteSearch(holdList, 0, holdList.size()-1, blockNum);
			if (ind2 != -1)
			{
				
				index.get(ind).remove(ind2+1);
			}
		}
	}
	
	/**
	 * Return the number of entries in the index
	 * @return
	 */
	public int size() {
		int size = 0;
		
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
