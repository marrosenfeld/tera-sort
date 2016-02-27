package terasort.hawk.iit.edu;

import java.util.PriorityQueue;

public class SubChunkBuffer {
	private PriorityQueue<SubChunk> buffer;
	private Integer capacity;

	public SubChunkBuffer(Integer capacity, Integer chunkSize, Integer subChunkSize) {
		super();
		this.capacity = capacity;
		buffer = new PriorityQueue<SubChunk>();

	}

	public synchronized void write(Integer subChunkIndex, Integer index, String subChunk, String writerName) {
		buffer.add(new SubChunk(subChunkIndex, index, subChunk));
	}

	public synchronized SubChunk read() throws InterruptedException {
		// buffer[index].setChunkIndex(buffer[index].getSubChunkIndex() + 1);
		return buffer.poll();
	}

	public Integer getCapacity() {
		return capacity;
	}

	public void setCapacity(Integer capacity) {
		this.capacity = capacity;
	}

	public void add(SubChunk subChunk) {
		buffer.add(subChunk);

	}

	public Integer getSize() {
		return this.buffer.size();
	}

}
