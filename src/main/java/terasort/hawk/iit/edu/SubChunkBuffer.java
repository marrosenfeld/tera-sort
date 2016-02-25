package terasort.hawk.iit.edu;

public class SubChunkBuffer {
	private SubChunk[] buffer;
	private Integer capacity;

	public SubChunkBuffer(Integer capacity, Integer chunkSize, Integer subChunkSize) {
		super();
		this.capacity = capacity;
		buffer = new SubChunk[capacity];
		for (int i = 0; i < capacity; i++) {
			buffer[i] = new SubChunk(0);
		}
	}

	public synchronized void write(Integer index, String subChunk, String writerName) {
		buffer[index].setContent(subChunk);
	}

	public synchronized String read(String readerName, Integer index) throws InterruptedException {
		// buffer[index].setChunkIndex(buffer[index].getSubChunkIndex() + 1);
		return buffer[index].getContent();
	}

	public SubChunk[] getBuffer() {
		return buffer;
	}

	public void setBuffer(SubChunk[] buffer) {
		this.buffer = buffer;
	}

	public Integer getCapacity() {
		return capacity;
	}

	public void setCapacity(Integer capacity) {
		this.capacity = capacity;
	}

}
