package terasort.hawk.iit.edu;

public class SubChunk implements Comparable<SubChunk> {

	Integer subChunkIndex;
	String content;
	Integer chunkIndex;

	public SubChunk(Integer subChunkIndex, Integer chunkIndex, String content) {
		super();
		this.subChunkIndex = subChunkIndex;
		this.chunkIndex = chunkIndex;
		this.content = content;
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

	public Integer getChunkIndex() {
		return chunkIndex;
	}

	public void setChunkIndex(Integer chunkIndex) {
		this.chunkIndex = chunkIndex;
	}

	@Override
	public int compareTo(SubChunk o) {
		return this.content.substring(0, 100).compareTo(o.getContent().substring(0, 100));
	}

}
