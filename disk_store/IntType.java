package disk_store;

import java.nio.ByteBuffer;
import java.util.Arrays;

// All IntType objects are the same, so IntType is a singleton

public class IntType extends FieldType {
	
	private IntType() {}
	
	private static final IntType instance = new IntType();
	
	public static IntType getInstance() {
		return instance;
	}

	@Override
	public int getLen() {
		return Integer.BYTES;
	}
	
	@Override
	public Field blankField() {
		return new IntField(0);
	}
	
	@Override
	public boolean equals(Object obj) {
		// there is a singleton class, so an object that is equal
		// to it must have the same address
		return obj == this;
	}

	@Override
	public int hashCode() {
		// this is a singleton class, so the single object hashes to 0
		return 0;
	}

	@Override
	public String toString() {
		return "int";
	}

}
