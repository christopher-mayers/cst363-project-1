package test;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import disk_store.IntField;
import disk_store.IntType;
import disk_store.Record;
import disk_store.Schema;

class IntFieldTest {
	
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
	
	// return a new record
	static Record createTestRecord() {
		return createTestRecord(1,2,3);
	}

	@Test
	void serializeDeserialize() {
		// create a record and serialize it to the buffer
		Record rec1 = createTestRecord();
		IntField field0 = (IntField)rec1.get(0);
		field0.setValue(20);
		ByteBuffer buf = ByteBuffer.allocate(12);
		rec1.serialize(buf, 0);

		// create a record of the same type, and deserialize it from the buffer
		Record rec2 = createTestRecord();
		rec2.deserialize(buf, 0);
		assertTrue(rec1.equals(rec2));
	}

}
