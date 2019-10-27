package disk_store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

/**
 * A relational schema, which can be thought of as the type of a record
 * @author Glenn
 *
 */
public class Schema extends Type {
	
	private List<String> fnames;	  // invariant: must be non-empty, and elements must be unique
	private List<FieldType> ftypes;   // invariant: must be same length as fnames
	String key;			              // invariant: must be in fnames
	
	private static final int maxFieldNameLength = 24;

	// create a new record type with the given integer key
	public Schema(String key, IntType keyType) {
		fnames = new ArrayList<String>();
		fnames.add(key);
		ftypes = new ArrayList<FieldType>();
		ftypes.add(keyType);
		this.key = key;
	}
	
	/**
	 * add a field
	 * @param fname
	 * @param ftype
	 */
	public void add(String fname, FieldType ftype) {
		if (fname.length() > maxFieldNameLength) {
			throw new IllegalArgumentException("field name "+fname+" is more than "+maxFieldNameLength+" in length");
		}
		fnames.add(fname);
		ftypes.add(ftype);
	}
	
	/**
	 * get the name of the key field
	 * @return
	 */
	public String getKey() { return key; }
	
	/**
	 * Return the index of the key field.
	 * @return
	 */
	public int getKeyIndex() { return 0; }
	
	/**
	 * Return the index of the field with the given name (or -1 if no such field).
	 */
	public int getFieldIndex(String fname) { return fnames.indexOf(fname); }
	
	/**
	 * Return the type of the ith field.
	 * @param i
	 * @return
	 */
	public FieldType getType(int i) {
		if (i < 0 || i >= ftypes.size()) {
			throw new IllegalArgumentException("No field i in schema: "+this);
		}
		return ftypes.get(i);
	}
	
	/**
	 * Return the type of the field with the given name, or null if no such
	 * field name in this schema.
	 * @param fieldName
	 * @return
	 */
	public FieldType getType(String fname) {
		int i = fnames.indexOf(fname);
		return (i < 0) ? null : ftypes.get(i); 
	}
	
	/**
	 * return the name of the ith field
	 * @param i
	 * @return
	 */
	public String getName(int i) {
		if (i < 0 || i >= ftypes.size()) {
			throw new IllegalArgumentException("No field i in schema: "+this);
		}
		return fnames.get(i);
	}
	
	/**
	 * return the number of fields in the record type
	 * @return
	 */
	public int size() { return fnames.size(); }
	
	/**
	 * return a new record with default values for all fields
	 * @return
	 */
	public Record blankRecord() {
		List<Field> fields = new ArrayList<Field>();
		for (FieldType ft : ftypes) {
			fields.add(ft.blankField());
		}
		Record rec = new Record(fields, this);
		return rec;
	}
	
	@Override
	public int getLen() {
		int totLen = 0;
		for (FieldType ft : ftypes) {
			totLen += ft.getLen();
		}
		return totLen;
	}
	
	/**
	 * serialize this to the given buffer, at the given position
	 */
	public void serialize(ByteBuffer buf, int index) {
		// layout: 
		// number of fields (int)
		// field1 name (string)
		// field1 type (int)
		// field2 name 
		// field2 type
		// etc.
		// The field type value is 0 for int, > 0 for string, with value
		// indicating max string length
		int numFields = fnames.size();
		buf.putInt(index, numFields);
		index += Integer.BYTES;
		
		for (int i = 0; i < numFields; i++) {
			StringUtils.serializeString(fnames.get(i), buf, index);
			index += maxFieldNameLength + Integer.BYTES;
			
			if (ftypes.get(i) instanceof IntType) {
				buf.putInt(index, 0);
				index += Integer.BYTES;
			} else if (ftypes.get(i) instanceof StringType) {
				buf.putInt(index, ((StringType)ftypes.get(i)).maxChars());
				index += Integer.BYTES;
			} else {
				throw new IllegalStateException("Unexpected field type "+ftypes.get(i));
			}
		}
	}
	
	/**
	 * create a RecordType from the bytes at the given buffer, at the given position
	 */
	public static Schema deserialize(ByteBuffer buf, int index) {
		int numFields = buf.getInt(index);
		index += Integer.BYTES;
		
		Schema recType = null;
		FieldType fieldType;
		for (int i = 0; i < numFields; i++) {
			String fieldName = StringUtils.deserializeString(buf, index);
			index += maxFieldNameLength + Integer.BYTES;
			
			int typeID = buf.getInt(index);
			index += Integer.BYTES;
			if (typeID == 0) {
				fieldType = IntType.getInstance();
			} else if (typeID > 0) { 
				fieldType = new StringType(typeID);
			} else {
				throw new IllegalStateException("Unexpected field type ID"+typeID);
			}
			
			if (recType == null) {
				if (!(fieldType instanceof IntType)) {
					throw new IllegalStateException("Key must be of type int");
				}
				recType = new Schema(fieldName, (IntType)fieldType);
			} else {
				recType.add(fieldName, fieldType);
			}
		}
		return recType;
	}

	@Override
	public String toString() {
		Joiner joiner = Joiner.on(", ");
		String s = "["+joiner.join(fnames)+"]";
		return s;
	}
}
