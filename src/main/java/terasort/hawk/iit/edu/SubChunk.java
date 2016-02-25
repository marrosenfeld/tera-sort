package terasort.hawk.iit.edu;

public class SubChunk {

	Integer subChunkIndex;
	String content;

	public SubChunk(Integer subChunkIndex) {
		super();
		this.subChunkIndex = subChunkIndex;

	}

	public Integer getSubChunkIndex() {
		return subChunkIndex;
	}

	public void setSubChunkIndex(Integer subChunkIndex) {
		this.subChunkIndex = subChunkIndex;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

}
