package test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.Lists;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import disk_store.DB;
import disk_store.HeapDB;
import disk_store.IntField;
import disk_store.IntType;
import disk_store.Record;
import disk_store.Schema;

class HeapDBTest {
	
	static Random rand;
	
	static void fixOpenFile() {
		// delete file if possible
		String dbFilename = "C:\\Users\\Chef\\eclipse-workspace\\project\\src\\test\\this.txt";
		File file = new File(dbFilename);
		file.delete();
	}
	
	// insert numRecs records into the database, with keys
	// 1..numRecs
	static void insertRecords(DB db, int numRecs) {
		for (int key=1; key <= numRecs; key++) {
			int field3 = rand.nextInt(20);
			db.insert(createTestRecord(key, key+1, field3));
		}
	}
	
	// return a new record with the given field values
	static Record createTestRecord(int a, int b, int c) {
		// create a schema with three integer fields, named 'a', 'b', 'c',
		// with 'a' the key
		Schema schema = new Schema("a", IntType.getInstance());
		schema.add("b", IntType.getInstance());
		schema.add("c", IntType.getInstance());

		IntField aval = new IntField(a);
		IntField bval = new IntField(b);
		IntField cval = new IntField(c);
		Record rec = new Record(Lists.newArrayList(aval, bval, cval), schema);
		return rec;
	}
	
	@Test
	void testHeapOps() {
		// test insert, delete, and lookup operations
		
		fixOpenFile();
		String dbFilename = "C:\\Users\\Chef\\eclipse-workspace\\project\\src\\test\\this.txt";
		
		Record rec1 = createTestRecord(1,2,3);
		Record rec2 = createTestRecord(2,3,4);
		Record rec3 = createTestRecord(3,4,5);
		Record rec4 = createTestRecord(4,5,6);
		HeapDB db = new HeapDB(dbFilename, rec1.getSchema());

		// test insert
		db.insert(rec1);
		db.insert(rec2);
		db.insert(rec3);
		db.insert(rec4);
		assertTrue(db.size() == 4);

		// test lookup
		Record rec = db.lookup(2);
		assertTrue(rec != null);
		rec = db.lookup(0);
		assertTrue(rec == null);

		// test delete
		db.delete(2);
		assertTrue(db.size() == 3);
		
		db.close();
	}
	
	@Test
	void testLookupTime() {
		// compare time to lookup records with/without an index
		fixOpenFile();
		String dbFilename = "C:\\Users\\Chef\\eclipse-workspace\\project\\src\\test\\this.txt";
		
		// create a new DB; use index to speed inserts
		Record rec = createTestRecord(0,1,2);
		HeapDB db = new HeapDB(dbFilename, rec.getSchema());
		db.createOrderedIndex();

		rand = new Random(42);  // set seed for repeatability
		int numRecords = 5000;
		long t1 = System.nanoTime();
		insertRecords(db, numRecords);
        long t2 = System.nanoTime();
        System.out.println("insert time: "+(t2 - t1)/1000000+" ms");
		assertTrue(db.size() == numRecords);

		// lookup testing
		int numLookups = 5000;
		long startTime, endTime, noIndexTime, indexTime;

		// lookup without index
		db.deleteIndex();
		startTime = System.nanoTime();
		for (int i = 0; i < numLookups; i++) {
			int key = rand.nextInt(numRecords) + 1;
			rec = db.lookup(key);
		}
		endTime = System.nanoTime();
		System.out.println("lookup time without index: "+(endTime - startTime)/1000000.0+" ms");
		
		// lookup with index
    	db.createOrderedIndex();
		startTime = System.nanoTime();
		for (int i = 0; i < numLookups; i++) {
			int key = rand.nextInt(numRecords) + 1;
			rec = db.lookup(key);
		}
		endTime = System.nanoTime();
		System.out.println("lookup time with index: "+(endTime - startTime)/1000000.0+" ms");
		
		db.close();
	}
	
	@Test
	void testPrint() {
		// test the print method
		
		fixOpenFile();

		Record rec1 = createTestRecord(1,2,3);
		Record rec2 = createTestRecord(2,3,4);
		Record rec3 = createTestRecord(3,4,5);
		Record rec4 = createTestRecord(4,5,6);

		// create a small DB
		String dbFilename = "C:\\Users\\Chef\\eclipse-workspace\\project\\src\\test\\this.txt";
		HeapDB db = new HeapDB(dbFilename, rec1.getSchema());
		db.insert(rec1);
		db.insert(rec2);
		db.insert(rec3);
		db.insert(rec4);
		
		assertTrue(db.size() == 4);
		System.out.println(db.toString());
		
		db.close();
 	}
	
	@Test
	void testLookupNonkey() {
		// test lookup operations on non-key fields
		
		fixOpenFile();
		String dbFilename = "C:\\Users\\Chef\\eclipse-workspace\\project\\src\\test\\this.txt";
		
		// create a new DB; use index on primary key to speed inserts
		rand = new Random(42);  // set seed for repeatability
		Record rec = createTestRecord(0,1,2);
		HeapDB db = new HeapDB(dbFilename, rec.getSchema());
		db.createOrderedIndex();
		int numRecords = 2000;
		insertRecords(db, numRecords);
		
		// lookup records with field c value of 3
		List<Record> recs = db.lookup("c", 3);
		assertTrue(recs.size() == 102);
		
		// create index on field c
		db.createOrderedIndex("c");
		
		// try lookups again, with the index
		recs = db.lookup("c", 3);
		assertTrue(recs.size() == 102);
		
		db.close();
	}
	
	// return a random number from 0 to 2, inclusive, where
	// 0 means insert, 1 means delete, and 2 means lookup
	// This function controls the proportions of the various
	// operations.  For example, usually we want more inserts
	// than deletes.
	static int randomOp() {
		int i = ThreadLocalRandom.current().nextInt(100);
		int op = (i < 50) ? 0 : (i < 75) ? 1 : 2;
		return op;
	}
	
	@Test
	void testManyOps() {
		// try many random inserts, deletes, and lookups to see
		// if anything breaks
		int numKeys = 20;
		int numTests = 10000;
		
		// create a new DB
		fixOpenFile();
		String dbFilename = "C:\\Users\\Chef\\eclipse-workspace\\project\\src\\test\\this.txt";
		rand = new Random(42);  // set seed for repeatability
		Record rec = createTestRecord(0,1,2);
		HeapDB db = new HeapDB(dbFilename, rec.getSchema());
		
		// optionally use an index on primary key
		db.createOrderedIndex();
		
		// initialize keyPresent: keyPresent[k] == 0 if key not present in DB
		boolean[] keyPresent = new boolean[numKeys];	
		
		for (int i = 0; i < numTests; i++) {
			int key = ThreadLocalRandom.current().nextInt(numKeys);
			int op = randomOp(); 
			if (op == 0) {
				// insert
				// System.out.println("insert "+key);
				rec = createTestRecord(key, 1, key);
				boolean result = db.insert(rec);
				// result should be false iff key was present before insert
				assertFalse(keyPresent[key]  && result);    // insert on existing record returned true
				assertFalse(!keyPresent[key] && !result);   // insert on absent record returned false
				keyPresent[key] = true;
			} else if (op == 1) {
				// delete
				// System.out.println("delete "+key);
				boolean result = db.delete(key);
				// result should be true iff key was present before insert
				assertFalse(keyPresent[key] && !result);    // delete on existing record returned false
				assertFalse(!keyPresent[key] && result);    // delete on absent record returned true
				keyPresent[key] = false;
			} else {
				// lookup
				// System.out.println("lookup "+key);
				rec = db.lookup(key);
				// result should be null iff the key is not present
				assertFalse((rec == null) && keyPresent[key]);         // lookup on existing record returned null
				assertFalse((rec != null) & !keyPresent[key]);         // lookup on absent record returned record
				if (rec != null) {
					assertFalse(((IntField)rec.get(2)).getValue() != key); // key does not equal value in third field
				}
			}
		}
		db.close();
	}
}