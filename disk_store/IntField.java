package disk_store;

import java.nio.ByteBuffer;

public class IntField extends Field {
	
	private int i;
	
	public IntField(int i) {
		this.i = i;
		this.type = IntType.getInstance();
	}
	
	public int getValue() { return i; }
	
	public void setValue(int i) { this.i = i; }
	
	@Override
	public void serialize(ByteBuffer buf, int index) {
		buf.putInt(index, i);
	}
	
	@Override
	public void deserialize(ByteBuffer buf, int index) {
		i = buf.getInt(index);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof IntField)) {
			return false;
		}
		IntField f = (IntField)obj;
		return f.getValue() == getValue();
	}

	@Override
	public int hashCode() {
		return i;
	}

	@Override
	public String toString() {
		return Integer.toString(i);
	}

}
