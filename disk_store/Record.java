package disk_store;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.Joiner;

/**
 * A database record (aka tuple)
 * 
 * @author Glenn
 *
 */
public class Record {
	
	List<Field> fields;
	Schema schema;   // invariant: size and types of schema must match fields
	
	/**
	 * Create a new record with the given fields and schema.
	 * 
	 * @param fields
	 * @param schema
	 */
	public Record(List<Field> fields, Schema schema) {
		this.fields = fields;
		this.schema = schema;
		if (fields.size() != schema.size()) {
			throw new IllegalArgumentException("Error: Number of fields does not match size of schema.");
		}
	}
	
	/**
	 * @return number of fields in the record
	 */
	public int size() {
		return fields.size();
	}
	
	/**
	 * get the schema of this record
	 * @return
	 */
	public Schema getSchema() {
		return schema;
	}
	
	/**
	 * get the ith field of the record
	 * @param i index of the attribute (first attribute has index 0)
	 */
	public Field get(int i) {	
		return fields.get(i);
	}
	
	public void set(int i, Field field) {
		// field must have the right type
		if (!(field.getType().equals(schema.getType(i)))) {
			throw new IllegalArgumentException("Field does not match type of ith field in schema");
		}
		fields.set(i, field);
	}
	
	/**
	 * get the value of the record's key field
	 * @return
	 */
	public int getKey() {
		Field keyField = fields.get(schema.getKeyIndex());
		if (!(keyField instanceof IntField)) {
			throw new IllegalStateException("key field expected to be an IntField");
		}
		int key = ((IntField)keyField).getValue();
		return key;
	}
		
	/**
	 * write the fields of this record to the given buffer, at the given position
	 * @param buf
	 * @param index
	 */
	public void serialize(ByteBuffer buf, int index) {
		for (Field field: fields) {
			field.serialize(buf, index);
			index += field.getType().getLen();
		}
	}

	/**
	 * set the fields of this record using values in the given buffer, at the given position
	 * @param buf
	 * @param index
	 */
	public void deserialize(ByteBuffer buf, int index) {
		for (Field field : fields) {
			field.deserialize(buf, index);
			index += field.getType().getLen();
		}
	}
	
	/**
	 * return true if this has has same number of records as as obj,
	 * and the records have the same values
	 */
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Record)) {
			return false;
		}
		
		// same number of records
		Record rec = (Record)obj;
		if (rec.size() != size()) {
			return false;
		}
		
		// fields are equal
		for (int i = 0; i < size(); i++) {
			if (!(this.get(i).equals(rec.get(i)))) {
				return false;
			}
		}
		
		return true;
	}
	
	public String toString() {
		Joiner joiner = Joiner.on(", ");
		String s = "("+joiner.join(fields)+")";
		return s;
	}
}
