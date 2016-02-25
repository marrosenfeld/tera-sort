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

		// records to order
		StringBuilder chunk = new StringBuilder();
		ChunkBuffer chunkBuffer = new ChunkBuffer(1);
		FinalChunkFileWriter finalChunkFileWriter = new FinalChunkFileWriter(chunkBuffer, "Final", "dataset_final",
				chunkSize, fileSize);
		finalChunkFileWriter.start();

		for (int i = 0; i < fileSize / recordSize; i++) {
			Record minRecord = null;
			Integer minRecordIdx = null;
			for (int j = 0; j < subChunkBuffer.getBuffer().length; j++) {
				if (!subChunkBuffer.getBuffer()[j].getContent().isEmpty()) {

					Record record = new Record(subChunkBuffer.getBuffer()[j].getContent().substring(0, recordSize));
					if (minRecord == null) {
						minRecord = record;
						minRecordIdx = j;
					} else {
						if (minRecord.compareTo(record) > 0) {
							minRecord = record;
							minRecordIdx = j;
						}
					}
				}
			}
			subChunkBuffer.getBuffer()[minRecordIdx]
					.setContent(subChunkBuffer.getBuffer()[minRecordIdx].getContent().substring(recordSize));

			if (subChunkBuffer.getBuffer()[minRecordIdx].getContent().isEmpty()) {
				// bring more from file

				subChunkBuffer.getBuffer()[minRecordIdx]
						.setSubChunkIndex(subChunkBuffer.getBuffer()[minRecordIdx].getSubChunkIndex() + 1);
				if (subChunkBuffer.getBuffer()[minRecordIdx].getSubChunkIndex() * this.getSubChunkSize() < chunkSize) {
					SubChunkFileReader reader = new SubChunkFileReader(subChunkBuffer,
							"/home/mrosenfeld/repo/tera-sort/dataset_tmp",
							minRecordIdx * chunkSize + (subChunkBuffer.getBuffer()[minRecordIdx].getSubChunkIndex()
									* this.getSubChunkSize()),
							1, chunkSize, this.getSubChunkSize());

					reader.run();
				}
			}
			chunk.append(minRecord.getValue());
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

	private List<SubChunkFileReader> getSubChunkFileReaders(Integer fileReaderThreads, Integer chunkSize,
			Integer fileSize, SubChunkBuffer subChunkBuffer) {
		List<SubChunkFileReader> subChunkFileReaders = new ArrayList<SubChunkFileReader>();
		Integer chunksPerThread = fileSize / chunkSize / fileReaderThreads;
		for (int i = 0; i < fileReaderThreads; i++) {
			SubChunkFileReader reader = new SubChunkFileReader(subChunkBuffer,
					"/home/mrosenfeld/repo/tera-sort/dataset_tmp", i * chunkSize * chunksPerThread, chunksPerThread,
					chunkSize, this.getSubChunkSize());
			subChunkFileReaders.add(reader);
		}
		if (fileSize / chunkSize % fileReaderThreads > 0) {
			SubChunkFileReader lastChunkFileReader = subChunkFileReaders.get(subChunkFileReaders.size() - 1);
			lastChunkFileReader.setSubChunkCount(
					lastChunkFileReader.getSubChunkCount() + (fileSize / chunkSize % fileReaderThreads));
		}
		return subChunkFileReaders;
	}

	public Integer getSubChunkSize() {
		return Math.min(chunkSize, recordSize * ((availableMemory / this.getChunkCount()) / recordSize));
		// return availableMemory / this.getChunkCount();
	}

	public Integer getChunkCount() {
		return fileSize / chunkSize;
	}
}
