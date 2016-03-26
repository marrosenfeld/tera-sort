package terasort.hawk.iit.edu;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mrosenfeld Performs the merge phase
 */
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
		SubChunkBuffer subChunkBuffer = new SubChunkBuffer(this.getChunkCount(), chunkSize, this.getSubChunkSize(),
				filePath);

		// fill buffer with 1 subchunk of each chunk
		List<SubChunkFileReader> subChunkFileReaders = getSubChunkFileReaders(fileReaderThreadsCount, chunkSize,
				fileSize, subChunkBuffer, filePath);
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
				chunkSize, fileSize, filePath);
		finalChunkFileWriter.start();

		String record = null;
		for (int i = 0; i < fileSize / recordSize; i++) {
			// get minimum record from subchunks

			try {
				record = subChunkBuffer.read();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}

			chunk.append(record);
			if (chunk.length() == chunkSize) {
				chunkBuffer.write(chunk.toString(), "Merger");
				chunk = new StringBuilder();
			}
		}
		try {
			finalChunkFileWriter.join();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

	}

	private List<SubChunkFileReader> getSubChunkFileReaders(Integer fileReaderThreads, Integer chunkSize, Long fileSize,
			SubChunkBuffer subChunkBuffer, String filePath) {
		List<SubChunkFileReader> subChunkFileReaders = new ArrayList<SubChunkFileReader>();
		Integer chunksPerThread = ((Long) (fileSize / chunkSize / fileReaderThreads)).intValue();
		for (int i = 0; i < fileReaderThreads; i++) {
			SubChunkFileReader reader = new SubChunkFileReader(subChunkBuffer, filePath + "dataset_tmp",
					Long.valueOf(i) * Long.valueOf(chunkSize) * Long.valueOf(chunksPerThread), chunksPerThread,
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
