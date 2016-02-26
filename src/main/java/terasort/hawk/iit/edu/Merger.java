package terasort.hawk.iit.edu;

import java.util.ArrayList;
import java.util.List;

public class Merger {
	private Long fileSize;
	private Integer chunkSize;
	private Long availableMemory;
	private Integer fileReaderThreadsCount;
	private Integer recordSize;
	private String filePath;

	public Merger(Long fileSize, Integer chunkSize, Long availableMemory, Integer fileReaderThreadsCount,
			Integer recordSize, String filePath) {
		super();
		this.fileSize = fileSize;
		this.chunkSize = chunkSize;
		this.availableMemory = availableMemory;
		this.fileReaderThreadsCount = fileReaderThreadsCount;
		this.recordSize = recordSize;
		this.filePath = filePath;
	}

	public void merge() {
		SubChunkBuffer subChunkBuffer = new SubChunkBuffer(this.getChunkCount(), chunkSize, this.getSubChunkSize());

		// fill buffer with 1 subchunk of each chunk
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
		System.out.println("buffer filled subchunk size: " + this.getSubChunkSize());
		// records to order
		StringBuilder chunk = new StringBuilder();
		ChunkBuffer chunkBuffer = new ChunkBuffer(1);
		FinalChunkFileWriter finalChunkFileWriter = new FinalChunkFileWriter(chunkBuffer, "Final", "dataset_final",
				chunkSize, fileSize);
		finalChunkFileWriter.start();

		SubChunk subChunk = null;
		String minKey = null;
		Integer minKeyIdx = null;
		for (int i = 0; i < fileSize / recordSize; i++) {
			// get minimum record from subchunks
			minKey = null;
			minKeyIdx = null;
			for (int j = 0; j < subChunkBuffer.getBuffer().length; j++) {
				if (!subChunkBuffer.getBuffer()[j].getContent().isEmpty()) {

					String key = subChunkBuffer.getBuffer()[j].getContent().substring(0, recordSize);
					if (minKey == null) {
						minKey = key;
						minKeyIdx = j;
					} else {
						if (minKey.compareTo(key) > 0) {
							minKey = key;
							minKeyIdx = j;
						}
					}
				}
			}

			// remove the first record from the subchunk
			subChunkBuffer.getBuffer()[minKeyIdx]
					.setContent(subChunkBuffer.getBuffer()[minKeyIdx].getContent().substring(recordSize));

			subChunk = subChunkBuffer.getBuffer()[minKeyIdx];
			if (subChunk.getContent().isEmpty()) {
				// bring more from file if already not read the whole chunk
				subChunk.setSubChunkIndex(subChunk.getSubChunkIndex() + 1);
				if (subChunk.getSubChunkIndex() * this.getSubChunkSize() < chunkSize) {
					SubChunkFileReader reader = new SubChunkFileReader(subChunkBuffer, filePath + "/dataset_tmp",
							minKeyIdx * chunkSize + (subChunk.getSubChunkIndex() * this.getSubChunkSize()), 1,
							chunkSize, this.getSubChunkSize());

					reader.run();
				}
			}
			chunk.append(minKey);
			if (chunk.length() == chunkSize) {
				chunkBuffer.write(chunk.toString(), "Merger");
				chunk = new StringBuilder();
			}
			if (chunk.length() % 10000 == 0)
				System.out.println(chunk.length());
		}
		try {
			finalChunkFileWriter.join();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

	}

	private List<SubChunkFileReader> getSubChunkFileReaders(Integer fileReaderThreads, Integer chunkSize, Long fileSize,
			SubChunkBuffer subChunkBuffer) {
		List<SubChunkFileReader> subChunkFileReaders = new ArrayList<SubChunkFileReader>();
		Integer chunksPerThread = ((Long) (fileSize / chunkSize / fileReaderThreads)).intValue();
		for (int i = 0; i < fileReaderThreads; i++) {
			SubChunkFileReader reader = new SubChunkFileReader(subChunkBuffer,
					"/home/mrosenfeld/repo/tera-sort/dataset_tmp", i * chunkSize * chunksPerThread, chunksPerThread,
					chunkSize, this.getSubChunkSize());
			subChunkFileReaders.add(reader);
		}
		if (fileSize / chunkSize % fileReaderThreads > 0) {
			SubChunkFileReader lastChunkFileReader = subChunkFileReaders.get(subChunkFileReaders.size() - 1);
			lastChunkFileReader.setSubChunkCount(lastChunkFileReader.getSubChunkCount()
					+ ((Long) (fileSize / chunkSize % fileReaderThreads)).intValue());
		}
		return subChunkFileReaders;
	}

	// get subchunk size (in bytes)
	public Integer getSubChunkSize() {
		return Math.min(chunkSize,
				((Long) (recordSize * ((availableMemory / this.getChunkCount()) / recordSize))).intValue());

	}

	public Integer getChunkCount() {
		return ((Long) (fileSize / chunkSize)).intValue();
	}
}
