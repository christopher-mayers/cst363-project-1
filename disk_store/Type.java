package disk_store;

import java.nio.ByteBuffer;

/**
 * A type, which can be a field type or a compound type
 * @author Glenn
 *
 */
public abstract class Type {
	// return the number of bytes needed to represent a value of this type
	public abstract int getLen();
	
    public abstract String toString();
}
