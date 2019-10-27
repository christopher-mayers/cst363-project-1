package disk_store;

import java.nio.ByteBuffer;

public class StringField extends Field {
	
	private String s;
	
	public StringField(String s, StringType st) {
		if (s.length() > st.maxChars()) {
			throw new IllegalArgumentException("string exceeds legal max length of "+st.maxChars());
		}
		this.s = s;
		this.type = st;
	}
	
	public String getValue() { return s; }

	@Override
	public void serialize(ByteBuffer buf, int index) {
		StringUtils.serializeString(s, buf, index);
	}

	@Override
	public void deserialize(ByteBuffer buf, int index) {
		s = StringUtils.deserializeString(buf, index);
	}

	/**
	 * return true if the given object is a StringField
	 * with the same value as this
	 */
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof StringField)) {
			return false;
		}
		StringField f = (StringField)obj;
		return f.getValue().equals(getValue());
	}

	@Override
	public int hashCode() {
		// note that this will hash two StringFields to the same value,
		// even if their types (max length) differ
		return s.hashCode();
	}

	@Override
	public String toString() {
		return s;
	}

}
