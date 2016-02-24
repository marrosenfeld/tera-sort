package terasort.hawk.iit.edu;

import java.util.ArrayList;

public class ChunkBuffer {
	private ArrayList<String> buffer;
	private Integer capacity;

	public ChunkBuffer(Integer capacity) {
		super();
		this.capacity = capacity;
		buffer = new ArrayList<String>();
	}

	public boolean isEmpty() {
		return buffer.isEmpty();
	}

	public boolean isFull() {
		return capacity.equals(buffer.size());
	}

	public synchronized void write(String chunk, String writerName) {
		while (this.isFull()) {
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		buffer.add(chunk);
		System.out.println("Data " + chunk + " produced by " + writerName);
		notifyAll();
	}

	public synchronized String read(String readerName) throws InterruptedException {
		while (isEmpty()) {
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		String chunk = buffer.remove(0);
		notifyAll();
		System.out.println("Data " + chunk + " consumed by " + readerName);
		return chunk;
	}
}
