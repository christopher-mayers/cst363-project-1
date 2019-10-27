package disk_store;

import java.nio.ByteBuffer;

public abstract class Field {
	
	protected Type type;
	
	/**
	 * return the type of the field
	 * @return
	 */
	public Type getType() { return type; }
	
	/**
	 * write the field as a sequence of bytes to position index in the given array
	 * @param buf a ByteBuffer
	 * @param index
	 */
    public abstract void serialize(ByteBuffer buf, int index);
    
	/**
	 * Set the field value using the value in the buffer at position index
	 * @param buf a ByteBuffer
	 * @param index location of the bytes for the field in buf
	 */
	public abstract void deserialize(ByteBuffer buf, int index);
	
	public abstract boolean equals(Object obj);
	
	public abstract int hashCode();
	
    public abstract String toString();
}
