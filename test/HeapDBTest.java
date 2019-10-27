package test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.Lists;

import org.junit.jupiter.api.AfterEach;
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
	static String dbFilename = "E:/Glenn/CSUMB/fall18/DB/temp3.txt";
	static Schema schema;
	
	@BeforeAll
	static void beforeAll() {
		// create a schema with three integer fields, named 'a', 'b', 'c',
		// with 'a' the key
		schema = new Schema("a", IntType.getInstance());
		schema.add("b", IntType.getInstance());
		schema.add("c", IntType.getInstance());
	}
	
	@BeforeEach
	void init() {
		fixOpenFile();
	}
	
	@AfterEach
	void wrapup() {
		// wrapup
	}
	
	static void fixOpenFile() {
		// delete file if possible
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
		IntField aval = new IntField(a);
		IntField bval = new IntField(b);
		IntField cval = new IntField(c);
		Record rec = new Record(Lists.newArrayList(aval, bval, cval), schema);
		return rec;
	}
	
	@Test
	void testHeapOps() {
		// test insert, delete, and lookup operations
		
		HeapDB db = new HeapDB(dbFilename, schema);

		// test insert
		Record rec1 = createTestRecord(1,2,3);
		Record rec2 = createTestRecord(2,3,4);
		Record rec3 = createTestRecord(3,4,5);
		Record rec4 = createTestRecord(4,5,6);
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
		
		// create a new DB; use index to speed inserts
		HeapDB db = new HeapDB(dbFilename, schema);
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
		Record rec;
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

		// create a small DB
		HeapDB db = new HeapDB(dbFilename, schema);

		Record rec1 = createTestRecord(1,2,3);
		Record rec2 = createTestRecord(2,3,4);
		Record rec3 = createTestRecord(3,4,5);
		Record rec4 = createTestRecord(4,5,6);
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
		
		int numRecords = 2000;

		// create a new DB; use index on primary key to speed inserts
		HeapDB db = new HeapDB(dbFilename, schema);
		db.createOrderedIndex();
		rand = new Random(42);  // set seed for repeatability
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
	
	@Test
	void testIndexDeleteMaintenance() {
		// test to make sure index maintenance works
		// after delete operations on search keys that are
		// not the primary key
		
		int numRecords = 500;

		// create a new DB; use index on primary key to speed inserts
		HeapDB db = new HeapDB(dbFilename, schema);
		db.createOrderedIndex();
		rand = new Random(42);  // set seed for repeatability
		insertRecords(db, numRecords);
		
		// create index on field c
		db.createOrderedIndex("c");
		
		// delete a bunch of records
		for (int i = 0; i < numRecords; i += 20) {
		   db.delete(i);
		}
		
		// lookup records with field c value of 3, using index
		List<Record> recs = db.lookup("c", 3);
		int m = recs.size();
		
		// try the lookup again, without the index
		db.deleteIndex("c");
		recs = db.lookup("c", 3);
		assertTrue(recs.size() == m);
		System.out.println("by indexed lookup: "+m+"; by sequential lookup: "+recs.size());
		
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
		
		rand = new Random(42);  // set seed for repeatability

		// create a new DB
		HeapDB db = new HeapDB(dbFilename, schema);
		
		// optionally use an index on primary key
		db.createOrderedIndex();
		
		// initialize keyPresent: keyPresent[k] == 0 if key not present in DB
		boolean[] keyPresent = new boolean[numKeys];	
		
		Record rec;
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
