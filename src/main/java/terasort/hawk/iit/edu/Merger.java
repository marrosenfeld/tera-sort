package terasort.hawk.iit.edu;

import java.util.ArrayList;
import java.util.List;

public class Merger extends Thread {
	private Integer fileSize;
	private Integer chunkSize;
	private Integer availableMemory;
	private Integer fileReaderThreadsCount;
	private Integer recordSize;

	public Merger(Integer fileSize, Integer chunkSize, Integer availableMemory, Integer fileReaderThreadsCount,
			Integer recordSize) {
		super();
		this.fileSize = fileSize;
		this.chunkSize = chunkSize;
		this.availableMemory = availableMemory;
		this.fileReaderThreadsCount = fileReaderThreadsCount;
		this.recordSize = recordSize;
	}

	@Override
	public void run() {
		SubChunkBuffer subChunkBuffer = new SubChunkBuffer(this.getChunkCount(), chunkSize, this.getSubChunkSize());
		List<SubChunkFileReader> subChunkFileReaders = getSubChunkFileReaders(fileReaderThreadsCount, chunkSize,
				fileSize, subChunkBuffer);
		for (SubChunkFileReader subChunkFileReader : subChunkFileReaders) {
			subChunkFileReader.start();
		}
		for (SubChunkFileReader subChunkFileReader : subChunkFileReaders) {
			try {
				subChunkFileReader.join();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		subChunkBuffer.hashCode();
	}

	private List<SubChunkFileReader> getSubChunkFileReaders(Integer fileReaderThreads, Integer chunkSize,
			Integer fileSize, SubChunkBuffer subChunkBuffer) {
		List<SubChunkFileReader> subChunkFileReaders = new ArrayList<SubChunkFileReader>();
		// Integer chunksPerThread = fileSize / chunkSize / fileReaderThreads;
		Integer chunksPerThread = 1;
		for (int i = 0; i < fileReaderThreads; i++) {
			SubChunkFileReader reader = new SubChunkFileReader(subChunkBuffer,
					"/home/mrosenfeld/repo/tera-sort/dataset_tmp", i * chunkSize * chunksPerThread, chunksPerThread,
					chunkSize, recordSize * (this.getSubChunkSize() / recordSize));
			subChunkFileReaders.add(reader);
		}
		// if (fileSize / chunkSize % fileReaderThreads > 0) {
		// SubChunkFileReader lastChunkFileReader =
		// subChunkFileReaders.get(subChunkFileReaders.size() - 1);
		// lastChunkFileReader.setSubChunkCount(
		// lastChunkFileReader.getSubChunkCount() + (fileSize / chunkSize %
		// fileReaderThreads));
		// }
		return subChunkFileReaders;
	}

	public Integer getSubChunkSize() {
		return availableMemory / this.getChunkCount();
	}

	public Integer getChunkCount() {
		return fileSize / chunkSize;
	}
}
