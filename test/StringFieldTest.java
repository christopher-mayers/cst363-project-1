package test;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import disk_store.IntField;
import disk_store.IntType;
import disk_store.Record;
import disk_store.Schema;
import disk_store.StringField;
import disk_store.StringType;

class StringFieldTest {

	@Test
	void serializeDeserialize() {
		// a schema with one int, one string
		Schema schema = new Schema("a", IntType.getInstance());
		StringType st = new StringType(12);
		schema.add("b", st);

		// create two records from the schema
		IntField aval = new IntField(1);
		StringField bval = new StringField("foo", st);
		Record rec1 = new Record(Lists.newArrayList(aval, bval), schema);

		IntField cval = new IntField(2);
		StringField dval = new StringField("bar", st);
		Record rec2 = new Record(Lists.newArrayList(cval, dval), schema);

		// serialize rec1, then deserialize as rec2
		ByteBuffer buf = ByteBuffer.allocate(64);
		rec1.serialize(buf, 0);
		rec2.deserialize(buf, 0);
		assertTrue(rec1.equals(rec2));

		// try again, with a different string value
		rec1 = new Record(Lists.newArrayList(aval, new StringField("", st)), schema);
		rec1.serialize(buf, 0);
		rec2.deserialize(buf,  0);
		assertTrue(rec1.equals(rec2));
	}

}
