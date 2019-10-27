package test;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import disk_store.Bitmap;

class BitmapTest {
	
	// test set and get methods on a 1 byte map
	@Test
	void oneByteMap() {
		Bitmap bmap = new Bitmap(new byte[1]);
		assertFalse(bmap.getBit(0));
		assertFalse(bmap.getBit(7));
		
		bmap.setBit(0, true);
		bmap.setBit(1, true);
		bmap.setBit(7, true);
		
		assertTrue(bmap.getBit(0));
		assertTrue(bmap.getBit(1));
		assertFalse(bmap.getBit(2));
		assertTrue(bmap.getBit(7));
		
		bmap.setBit(1, false);
		
		assertFalse(bmap.getBit(1));
	}
	
	// test get and set methods on a 2 byte map
	@Test
	void twoByteMap() {
		Bitmap bmap = new Bitmap(new byte[2]);
		bmap.setBit(0, true);
		bmap.setBit(5, true);
		bmap.setBit(15, true);
		
		assertTrue(bmap.getBit(0));
		assertFalse(bmap.getBit(1));
		assertTrue(bmap.getBit(15));
		assertFalse(bmap.getBit(14));
	}
	
	// test get and set methods on a map using constructor to initialize bits
	@Test
	void initializedMap() {
		Bitmap bmap = new Bitmap(new byte[] { 0x00, 0x05 } );
		assertFalse(bmap.getBit(0));
		assertFalse(bmap.getBit(7));
		assertFalse(bmap.getBit(8));
		assertTrue(bmap.getBit(13));
		assertFalse(bmap.getBit(14));
		assertTrue(bmap.getBit(15));
	}
	
	// test firstZero method
	@Test
	void firstZero() {
		Bitmap bmap = new Bitmap(new byte[2]);
		bmap.setBit(0, true);
		bmap.setBit(5, true);
		bmap.setBit(15, true);
		int i = bmap.firstZero();
		assertEquals(i, 1);
	}
	
	// test Bitmap initialization for all bits turned on
	@Test
	void initializedToOnes() {
		Bitmap bmap = new Bitmap(new byte[] { -0x01, -0x01 } );
		assertTrue(bmap.getBit(0));
		assertTrue(bmap.getBit(7));
		assertTrue(bmap.getBit(8));
		assertTrue(bmap.getBit(15));
		int i = bmap.firstZero();
		assertEquals(i, -1);
	}

	// test that clear method zeros all bits
	@Test
	void clearShouldZeroBits() {
		Bitmap bmap = new Bitmap(new byte[] { 0x06 } );
		assertTrue(bmap.getBit(5));
		bmap.clear();
		assertFalse(bmap.getBit(5));
	}
	
	// test size
	@Test
	void testSize() {
		Bitmap bmap = new Bitmap(new byte[] { 0x06, 0x0a } );
		assertEquals(bmap.size(), 16);
	}
	
	
}
