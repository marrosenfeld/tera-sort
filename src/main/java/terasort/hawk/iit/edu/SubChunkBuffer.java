package terasort.hawk.iit.edu;

public class SubChunkBuffer {
	private SubChunk[] buffer;
	private Integer capacity;

	public SubChunkBuffer(Integer capacity, Integer chunkSize, Integer subChunkSize) {
		super();
		this.capacity = capacity;
		buffer = new SubChunk[capacity];
		for (int i = 0; i < capacity; i++) {
			buffer[i] = new SubChunk(i, 0, chunkSize, subChunkSize);
		}
	}

	public synchronized void write(Integer index, String subChunk, String writerName) {
		buffer[index].setContent(subChunk);
		System.out.println("Data " + subChunk + " produced by " + writerName);
	}

	public synchronized String read(String readerName, Integer index) throws InterruptedException {
		buffer[index].setChunkIndex(buffer[index].getSubChunkIndex() + 1);
		return buffer[index].getContent();
		// while (isEmpty()) {
		// try {
		// wait();
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		// }
		// String chunk = buffer.remove(0);
		// Thread.sleep(1000);
		// notifyAll();
		// System.out.println("Data " + chunk + " consumed by " + readerName);
		// return chunk;
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
