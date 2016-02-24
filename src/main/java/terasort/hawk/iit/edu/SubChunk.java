package terasort.hawk.iit.edu;

public class SubChunk {
	Integer chunkIndex;
	Integer subChunkIndex;
	Integer chunkSize;
	Integer subChunkSize;
	// List<Record> records;
	String content;

	public SubChunk(Integer chunkIndex, Integer subChunkIndex, Integer chunkSize, Integer subChunkSize) {
		super();
		this.chunkIndex = chunkIndex;
		this.subChunkIndex = subChunkIndex;
		this.chunkSize = chunkSize;
		this.subChunkSize = subChunkSize;
	}

	public Integer getChunkIndex() {
		return chunkIndex;
	}

	public void setChunkIndex(Integer chunkIndex) {
		this.chunkIndex = chunkIndex;
	}

	public Integer getSubChunkIndex() {
		return subChunkIndex;
	}

	public void setSubChunkIndex(Integer subChunkIndex) {
		this.subChunkIndex = subChunkIndex;
	}

	public Integer getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(Integer chunkSize) {
		this.chunkSize = chunkSize;
	}

	public Integer getSubChunkSize() {
		return subChunkSize;
	}

	public void setSubChunkSize(Integer subChunkSize) {
		this.subChunkSize = subChunkSize;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	// public List<Record> getRecords() {
	// return records;
	// }
	//
	// public void setRecords(List<Record> records) {
	// this.records = records;
	// }

}
