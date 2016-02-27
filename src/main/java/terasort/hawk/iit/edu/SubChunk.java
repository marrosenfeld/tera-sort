package terasort.hawk.iit.edu;

public class SubChunk implements Comparable<SubChunk> {

	private Integer subChunkIndex;
	private String content;
	private Integer chunkIndex;
	private String firstRecordKey;
	private String firstRecord;

	public SubChunk(Integer subChunkIndex, Integer chunkIndex, String content) {
		super();
		this.subChunkIndex = subChunkIndex;
		this.chunkIndex = chunkIndex;
		this.content = content;
		this.firstRecord = content.substring(0, 100);
		this.firstRecordKey = firstRecord.substring(0, 10);
	}

	public Integer getSubChunkIndex() {
		return subChunkIndex;
	}

	public void setSubChunkIndex(Integer subChunkIndex) {
		this.subChunkIndex = subChunkIndex;
	}

	public String getFirstRecordKey() {
		return this.firstRecordKey;
	}

	public void removeFirstRecord() {
		this.content = this.content.substring(100);
		if (!this.content.isEmpty()) {
			this.firstRecord = this.content.substring(0, 100);
			this.firstRecordKey = firstRecord.substring(0, 10);
		}
	}

	public Boolean isEmpty() {
		return this.content.isEmpty();
	}

	public String getFirstRecord() {
		return this.firstRecord;
	}

	public Integer getChunkIndex() {
		return chunkIndex;
	}

	public void setChunkIndex(Integer chunkIndex) {
		this.chunkIndex = chunkIndex;
	}

	@Override
	public int compareTo(SubChunk o) {
		return this.firstRecordKey.compareTo(o.getFirstRecordKey());
	}

}
