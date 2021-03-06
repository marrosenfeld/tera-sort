package terasort.hawk.iit.edu;

import java.util.PriorityQueue;

public class SubChunkBuffer {
	private class Aux implements Comparable<Aux> {
		private String key;
		private Integer index;

		public Aux(String key, Integer index) {
			super();
			this.key = key;
			this.index = index;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public Integer getIndex() {
			return index;
		}

		public void setIndex(Integer index) {
			this.index = index;
		}

		@Override
		public int compareTo(Aux o) {
			return this.key.compareTo(o.getKey());
		}

	}

	private SubChunk[] buffer;
	private PriorityQueue<Aux> queue;
	private Integer capacity;
	private Integer chunkSize;
	private Integer subChunkSize;
	private String filePath;

	public SubChunkBuffer(Integer capacity, Integer chunkSize, Integer subChunkSize, String filePath) {
		super();
		this.capacity = capacity;
		queue = new PriorityQueue<Aux>();
		buffer = new SubChunk[capacity];
		this.subChunkSize = subChunkSize;
		this.chunkSize = chunkSize;
		this.filePath = filePath;
	}

	public synchronized void write(Integer subChunkIndex, Integer index, String subChunk, String writerName) {
		buffer[index] = new SubChunk(subChunkIndex, index, subChunk);
		queue.add(new Aux(subChunk, index));
	}

	public synchronized String read() throws InterruptedException {

		Aux aux = queue.poll();
		SubChunk subChunk = this.buffer[aux.getIndex()];
		String record = subChunk.getFirstRecord();
		subChunk.removeFirstRecord();
		if (subChunk.isEmpty()) {
			// bring more from file if already not read the whole chunk
			subChunk.setSubChunkIndex(subChunk.getSubChunkIndex() + 1);
			if (subChunk.getSubChunkIndex() * subChunkSize < chunkSize) {
				SubChunkFileReader reader = new SubChunkFileReader(this, filePath + "/dataset_tmp",
						Long.valueOf(subChunk.getChunkIndex()) * Long.valueOf(chunkSize)
								+ (subChunk.getSubChunkIndex() * subChunkSize),
						1, chunkSize, subChunkSize);

				reader.run();
			}
		} else {
			queue.add(new Aux(subChunk.getFirstRecordKey(), aux.getIndex()));
		}
		return record;
	}

	public Integer getCapacity() {
		return capacity;
	}

	public void setCapacity(Integer capacity) {
		this.capacity = capacity;
	}

}
