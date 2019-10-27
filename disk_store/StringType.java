package disk_store;

/**
 * A string (varchar) type.
 * 
 * @author Glenn
 *
 */

public class StringType extends FieldType {
	
	// maximum allowed string length
	public static final int maxCharLimit = 64;
	private int maxChars;
	
	public StringType(int maxChars) {
		if (maxChars == 0) {
			throw new IllegalArgumentException("a StringType must allow for more than 0 characters");
		}
		if (maxChars > maxCharLimit) {
			throw new IllegalArgumentException("max characters in a string is "+maxCharLimit);
		}
		this.maxChars = maxChars;
	}
	
	public int maxChars() { return maxChars; }

	@Override
	public Field blankField() {
		return new StringField("", this);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof StringType)) {
			return false;
		}
		StringType st = (StringType)obj;
		return st.maxChars == maxChars;
	}

	@Override
	public int getLen() {
		// on-disk representation requires maxChar bytes, plus 4 bytes to store string length as Int
		return maxChars + Integer.BYTES;
	}

	@Override
	public int hashCode() {
		// the only difference between string types is maxChars
		return maxChars;
	}
	
	@Override
	public String toString() {
		return "string("+maxChars+")";
	}

}
