package disk_store;

import java.nio.ByteBuffer;

/**
 * A buffer to read/write file blocks.
 * A BlockBuffer is intended to be an easier-to-use ByteBuffer.
 * 
 * @author Glenn
 *
 */
public class BlockBuffer {
	
	private int blockSize;
	ByteBuffer buffer;

	/**
	 * create a new, empty buffer
	 */
	public BlockBuffer(int blockSize) {
		buffer = ByteBuffer.allocate(blockSize);
		this.blockSize = blockSize;
	}
	
	/**
	 * reset the buffer location to the beginning of the buffer
	 */
	public void reset() {
		buffer.clear();
	}
	
    /**
     * write the given integer value into the buffer at the current buffer position
     * @param value the integer value to be added
     */
	public void put(int value) {
		buffer.putInt(value);
	}
	
	/**
	 * read the integer at the current buffer position
	 * @return
	 */
	public int get() {
		return buffer.getInt();
	}
	
	/**
	 * write a byte to the specified absolute location in the buffer
	 * @param index index into the buffer
	 * @param b byte to be written
	 */
	public void put(int index, byte b) {
		buffer.put(index, b);
	}
	
	/**
	 * get a byte from the specified absolute location in the buffer
	 * @param index index into the buffer
	 * @return byte at position i in buffer
	 */
	public byte get(int index) {
		return buffer.get(index);
	}
	
	/**
	 * write an int to the specified absolute location in the buffer
	 * @param index
	 * @param value
	 */
	public void putInt(int index, int value) {
		buffer.putInt(index, value);
	}
	
	/**
	 * get an int from the specified absolute location in the buffer
	 * @param index
	 * @return
	 */
	public int getInt(int index) {
		return buffer.getInt(index);
	}
	
	public String toString() {
		return buffer.toString();
	}

}
