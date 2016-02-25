package terasort.hawk.iit.edu;

public class Record implements Comparable<Record> {
	String value;

	public Record(String value) {
		super();
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getKey() {
		return this.value.substring(0, 10);
	}

	public int compareTo(Record anotherRecord) {
		String anotherRecordKey = ((Record) anotherRecord).getKey();
		return this.getKey().compareTo(anotherRecordKey);
	}

	@Override
	public String toString() {
		return value;
	}

}
