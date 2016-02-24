package terasort.hawk.iit.edu;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;

public class ChunkMemorySorter extends Thread {
	private ChunkBuffer chunkBuffer;
	private Integer recordSize;
	private Integer chunkCount;
	private Integer chunkSize;
	private RecordQuickSort recordQuickSort;
	private ChunkBuffer orderedChunkBuffer;

	public ChunkMemorySorter(ChunkBuffer chunkBuffer, String threadName, Integer recordSize, Integer chunkCount,
			Integer chunkSize, ChunkBuffer orderedChunkBuffer) {
		this.chunkBuffer = chunkBuffer;
		setName(threadName);
		this.chunkCount = chunkCount;
		this.recordSize = recordSize;
		this.chunkSize = chunkSize;
		this.recordQuickSort = new RecordQuickSort();
		this.orderedChunkBuffer = orderedChunkBuffer;
	}

	@Override
	public void run() {
		for (int i = 0; i < chunkCount; i++) {
			try {
				String chunk = chunkBuffer.read(this.getName());
				Record[] records = this.splitChunk(chunk, chunkSize, recordSize);
				recordQuickSort.sort(records);
				orderedChunkBuffer.write(StringUtils.join(records), this.getName());
				System.out.println(this.getName() + " read and sort: " + Arrays.asList(records));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private Record[] splitChunk(String chunk, Integer chunkSize, Integer recordSize) {
		Record[] records = new Record[chunkSize / recordSize];
		for (int i = 0; i < chunkSize / recordSize; i++) {
			String recordString = chunk.substring(i * recordSize, (i * recordSize) + recordSize);
			records[i] = new Record(recordString);
		}
		return records;
	}

	public ChunkBuffer getChunkBuffer() {
		return chunkBuffer;
	}

	public void setChunkBuffer(ChunkBuffer chunkBuffer) {
		this.chunkBuffer = chunkBuffer;
	}

	public Integer getRecordSize() {
		return recordSize;
	}

	public void setRecordSize(Integer recordSize) {
		this.recordSize = recordSize;
	}

	public Integer getChunkCount() {
		return chunkCount;
	}

	public void setChunkCount(Integer chunkCount) {
		this.chunkCount = chunkCount;
	}

}
