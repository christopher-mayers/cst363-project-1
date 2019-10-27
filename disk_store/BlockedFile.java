package disk_store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * A file that can be accessed randomly by block.
 * All file reads and writes are through a built-in, block-sized buffer,
 * using methods putBuf and getBuf.  Also, in all file reads and
 * writes a file block number must be specified.
 * 
 * Typical process for writing to file:
 * bf.resetBuf();  // in normal use cases this isn't needed
 * bf.put(1);
 * bf.put(2);
 * bf.write();
 * 
 * Typical process for reading from a file:
 * bf.read();
 * int a = bf.get();
 * int b = bf.get();
 * 
 * @author Glenn
 *
 */

/* Implementation notes:
 *  - Efficiency may be improved by taking advantage of the memory
 *    mapping supported by FileChannel.
 *  - Note that class BlockBuffer is no longer used here.
 */

public class BlockedFile {
	
	private FileChannel fc;
	private long lastBlockIndex;
	static final int blockSize = 1024*4;
	
	// private constructor
	private BlockedFile(FileChannel fc, long lastBlockIndex) {
		this.fc = fc;
		this.lastBlockIndex = lastBlockIndex;
	}
	
	/** 
	 * create a new blocked file
	 * @param filename
	 */
	public BlockedFile(String filename) {
		this.lastBlockIndex = 0;
		Path file = Paths.get(filename);
		try {
			// create a file channel
			fc = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * open an existing blocked file for reading and writing
	 * @param filename
	 */
	public static BlockedFile open(String filename) {
		FileChannel fc = null;
		long sz = 0;
		Path file = Paths.get(filename);
		try {
			fc = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
			sz = fc.size();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if (sz % blockSize != 0) {
			throw new IllegalStateException("not a blocked file: file does not have an integral number of blocks");
		}
		
		BlockedFile bf = new BlockedFile(fc, sz % blockSize);
		return bf;
	}
	
	/**
	 * close a blocked file
	 */
	public void close() {
		try {
			fc.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// return a new buffer for use with this file
	public BlockBuffer getBuffer() {
		BlockBuffer buf = new BlockBuffer(blockSize);
		return buf;
	}
	
	/**
	 * read the block at the specified block index into buffer
	 * @param index block index
	 * @param b a BlockBuffer
	 * @return
	 */
	public int read(int index, BlockBuffer buf) {
		buf.reset();
		int numBytesRead = 0;
		try {
			fc.position(index * blockSize);
			numBytesRead = fc.read(buf.buffer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 		
		// reading changes the buffer's position value
		buf.reset();
		return numBytesRead;
	}

    /**
     * write the buffer contents to the specified block index
     * @param index block index
     * @param b block
     */
	public void write(int index, BlockBuffer buf) {
		buf.reset();
		try {
			fc.position(index * blockSize);
			fc.write(buf.buffer);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalStateException("Error: can't write block to file");
		}
		// writing changes the buffer's position value
		buf.reset();
		if (index > lastBlockIndex) {
			lastBlockIndex = index;
		}
	}

	public long size() {
		try {
			return fc.size();
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	public int blockSize() {
		return blockSize;
	}

	public long getLastBlockIndex() {
		return lastBlockIndex;
	}
	
	public String toString() {
		try {
			return "blocked file of size "+fc.size();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
	
}
