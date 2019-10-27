package disk_store;

/**
 * A bitmap, implemented as a byte array.
 * 
 * Note that java bytes are signed, so values range
 * from -128 to 127.  A byte of all 1's is -0x01.
 * 
 * @author Glenn
 *
 */

public class Bitmap {
	private byte[] bytes;
	private int numBytes;
	private final byte ONES = -0x01;	// 8 bits of 1s
	
	// Create a new bitmap.  The numBytes argument is provided
	// because some applications may not want to use the entire
	// byte array for the bitmap.
	public Bitmap(byte[] bytes, int numBytes) {
		this.bytes = bytes;
		this.numBytes = numBytes;
	}
	
	// alternative constructor when all bits in the byte array are to be used
	public Bitmap(byte[] bytes) {
		this(bytes, bytes.length);
	}
	
	// alternative constructor when initialization to 0 is desired
	public Bitmap(int numBytes) {
		this(new byte[numBytes], numBytes);
	}
	
	// number of bits in this bitmap
	public int size() { return numBytes * Byte.SIZE; }
	
	// set all bits in the bitmap to 0
	public void clear() {
		for (int i = 0; i < numBytes; i++) {
			bytes[i] = 0;
		}
	}
	
	// return true iff the ith bit is 1
	public boolean getBit(int i) {
		// ib is in index of the byte that contains the ith bit
		int ib = i / Byte.SIZE;
		if (ib >= numBytes) {
			throw new IllegalArgumentException("getting bit larger than bit map (byte number is "+ib+")");
		}
		
		// get the bit we need from that byte
		return getBit(bytes[ib], i - ib*Byte.SIZE);
	}
	
	// set the ith bit to 1 (if bit) or 0 (if !bit)
	public void setBit(int i, boolean bit) {
		// ib is in index of the byte that contains the ith bit
		int ib = i / Byte.SIZE;
		if (ib >= numBytes) {
			throw new IllegalArgumentException("bit i = "+i+" is larger than "+ib);
		}
		
		// set the bit we need with that byte, and update buffer
		bytes[ib] = setBit(bytes[ib], i - ib*Byte.SIZE, bit);
	}
	
	// return the index of the first bit that is 0
	// return -1 if no such bit
	public int firstZero() {
		for (int i = 0; i < numBytes; i++) {
			if (bytes[i] != ONES) {
				// some block is free; find the index of the first zero bit
				for (int j = 0; j < Byte.SIZE; j++) {
					if (!getBit(bytes[i], j)) {
						return i*Byte.SIZE + j;
					}
				}
				throw new IllegalStateException("no zero bit found");
			}
		}
		return -1;
	}
	
	// return true iff the ith bit of b is 1
	private boolean getBit(byte b, int i) {
		return ((b >> (7 - i)) & 1) != 0;
	}
	
	// return b except with bit i set to 1 (if bit) or 0 (if !bit)
	private byte setBit(byte b, int i, boolean bit) {
		if (bit) {
			b |= (1 << (7-i));
		} else {
			b &= ~(1 << (7-i));
		}
		return b;
	}
	
	// return the first bytes of the bitmap as a string
	public String toString() {
		int maxBytes = Math.min(12, numBytes);   // show up to 12 bytes
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < maxBytes; i++) {
			for (int j = 0; j < Byte.SIZE; j++) {
				sb.append(getBit(i*Byte.SIZE + j) ? "1" : "0");
			}
			sb.append(" ");
		}
		if (maxBytes < numBytes) {
			sb.append(" ...\n");
		}
		return sb.toString();
	}
	
}
