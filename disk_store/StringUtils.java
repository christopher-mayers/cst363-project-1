package disk_store;

import java.nio.ByteBuffer;

public class StringUtils {
	
	// write the string s to the buffer at position index
	public static void serializeString(String s, ByteBuffer buf, int index) {
		// write the length of the string followed by the bytes in the string
		
		// set position manually as there's no absolute put operation for byte arrays
		buf.position(index);
		
		// see stackoverflow.com/questions/24633980/java-nio-bytebuffer-put-and-get-strings
		byte[] bytes = s.getBytes();
		buf.putInt(bytes.length);
		buf.put(bytes);
	}
	
	// read the string s from the buffer at position index
	public static String deserializeString(ByteBuffer buf, int index) {
		// read the length of the string followed by the bytes in the string
		
		buf.position(index);
		
		int len = buf.getInt();
		byte[] bytes = new byte[len];
		buf.get(bytes);
		String s = new String(bytes);
		return s;
	}
}
