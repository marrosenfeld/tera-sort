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
				chunkSize, fileSize);
		finalChunkFileWriter.start();

		SubChunk subChunk = null;
		for (int i = 0; i < fileSize / recordSize; i++) {
			// get minimum record from subchunks

			try {
				subChunk = subChunkBuffer.read();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}

			// remove the first record from the subchunk
			String record = subChunk.content.substring(0, recordSize);
			subChunk.setContent(subChunk.getContent().substring(recordSize));

			if (subChunk.getContent().isEmpty()) {
				// bring more from file if already not read the whole chunk
				subChunk.setSubChunkIndex(subChunk.getSubChunkIndex() + 1);
				if (subChunk.getSubChunkIndex() * this.getSubChunkSize() < chunkSize) {
					SubChunkFileReader2 reader = new SubChunkFileReader2(subChunkBuffer, filePath + "/dataset_tmp",
							subChunk.getChunkIndex() * chunkSize
									+ (subChunk.getSubChunkIndex() * this.getSubChunkSize()),
							1, chunkSize, this.getSubChunkSize());

					reader.run();
				}
			} else {
				subChunkBuffer.add(subChunk);
			}
			chunk.append(record);
			if (chunk.length() == chunkSize) {
				chunkBuffer.write(chunk.toString(), "Merger");
				chunk = new StringBuilder();
			}
			if (chunk.length() % 100000 == 0) {
				System.out.println(chunk.length());
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
					i * chunkSize * chunksPerThread, chunksPerThread, chunkSize, this.getSubChunkSize());
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
