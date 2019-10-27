package disk_store;

import java.nio.ByteBuffer;

public abstract class FieldType extends Type {

	/**
	 * return a new field with default value
	 * @return
	 */
	public abstract Field blankField();
	
	@Override
	public abstract boolean equals(Object obj);
	
	@Override
	public abstract String toString();
}
