package terasort.hawk.iit.edu;

import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * @author mrosenfeld SubChunk of a Chunk
 */
public class SubChunk implements Comparable<SubChunk> {

	private Integer subChunkIndex;
	private List<String> records;
	private Integer chunkIndex;
	private String firstRecordKey;
	private String firstRecord;

	public SubChunk(Integer subChunkIndex, Integer chunkIndex, String content) {
		super();
		this.subChunkIndex = subChunkIndex;
		this.chunkIndex = chunkIndex;

		this.firstRecord = content.substring(0, 100);
		this.firstRecordKey = firstRecord.substring(0, 10);
		Iterable<String> recordStrings = Splitter.fixedLength(100).split(content);

		records = Lists.newArrayList(recordStrings);

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
		this.records.remove(0);

		if (!this.isEmpty()) {
			this.firstRecord = this.records.get(0);
			this.firstRecordKey = this.firstRecord.substring(0, 10);
		}
	}

	public Boolean isEmpty() {
		return this.records.isEmpty();
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
