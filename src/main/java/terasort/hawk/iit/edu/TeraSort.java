package terasort.hawk.iit.edu;

import java.util.ArrayList;
import java.util.List;

public class TeraSort {

	public static void main(String[] args) {
		// ProducerConsumerProblem pcp = new ProducerConsumerProblem();
		Integer fileReaderThreads = Integer.valueOf(args[0]);
		Integer memoryThreads = Integer.valueOf(args[1]);
		Integer chunkSize = Integer.valueOf(args[2]);
		Integer recordSize = Integer.valueOf(args[3]);
		Integer fileSize = Integer.valueOf(args[4]);
		Integer bufferSize = Integer.valueOf(args[5]);

		ChunkBuffer chunkBuffer = new ChunkBuffer(bufferSize);
		ChunkBuffer orderedChunkBuffer = new ChunkBuffer(bufferSize);

		// sort phase

		List<ChunkFileReader> chunkFileReaders = getChunkFileReaders(fileReaderThreads, chunkSize, fileSize,
				chunkBuffer);
		List<ChunkMemorySorter> chunkMemorySorters = getChunkMemorySorters(memoryThreads, fileSize, chunkSize,
				recordSize, chunkBuffer, orderedChunkBuffer);
		List<ChunkFileWriter> chunkFileWriters = getChunkFileWriters(fileReaderThreads, chunkSize, fileSize,
				orderedChunkBuffer);

		for (ChunkFileReader chunkFileReader : chunkFileReaders) {
			chunkFileReader.start();
		}
		for (ChunkMemorySorter chunkMemorySorter : chunkMemorySorters) {
			chunkMemorySorter.start();
		}
		for (ChunkFileWriter chunkFileWriter : chunkFileWriters) {
			chunkFileWriter.start();
		}

		try {
			for (ChunkFileReader chunkFileReader : chunkFileReaders) {
				chunkFileReader.join();
			}
			for (ChunkMemorySorter chunkMemorySorter : chunkMemorySorters) {
				chunkMemorySorter.join();
			}
			for (ChunkFileWriter chunkFileWriter : chunkFileWriters) {
				chunkFileWriter.join();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// merge phase
		System.out.println("End");
	}

	private static List<ChunkFileWriter> getChunkFileWriters(Integer fileWriterThreads, Integer chunkSize,
			Integer fileSize, ChunkBuffer orderedChunkBuffer) {
		List<ChunkFileWriter> chunkFileWriters = new ArrayList<ChunkFileWriter>();
		Integer chunksPerThread = fileSize / chunkSize / fileWriterThreads;
		for (int i = 0; i < fileWriterThreads; i++) {
			ChunkFileWriter writer = new ChunkFileWriter(orderedChunkBuffer, "fileReader " + i,
					"/home/mrosenfeld/repo/tera-sort/dataset_tmp", i * chunkSize * chunksPerThread, chunksPerThread,
					chunkSize);
			chunkFileWriters.add(writer);
		}
		if (fileSize / chunkSize % fileWriterThreads > 0) {
			ChunkFileWriter lastChunkFileWriter = chunkFileWriters.get(chunkFileWriters.size() - 1);
			lastChunkFileWriter
					.setChunkCount(lastChunkFileWriter.getChunkCount() + (fileSize / chunkSize % fileWriterThreads));
		}
		return chunkFileWriters;

	}

	private static List<ChunkMemorySorter> getChunkMemorySorters(Integer memoryThreads, Integer fileSize,
			Integer chunkSize, Integer recordSize, ChunkBuffer chunkBuffer, ChunkBuffer orderedChunkBuffer) {

		List<ChunkMemorySorter> chunkMemorySorters = new ArrayList<ChunkMemorySorter>();

		Integer chunksPerThread = fileSize / chunkSize / memoryThreads;

		for (int i = 0; i < memoryThreads; i++) {
			ChunkMemorySorter sorter = new ChunkMemorySorter(chunkBuffer, "MemorySorter" + i, recordSize,
					chunksPerThread, chunkSize, orderedChunkBuffer);
			chunkMemorySorters.add(sorter);
		}

		if (fileSize / chunkSize % memoryThreads > 0) {
			ChunkMemorySorter lastChunkMemorySorter = chunkMemorySorters.get(chunkMemorySorters.size() - 1);

			lastChunkMemorySorter
					.setChunkCount(lastChunkMemorySorter.getChunkCount() + (fileSize / chunkSize % memoryThreads));
		}

		return chunkMemorySorters;
	}

	private static List<ChunkFileReader> getChunkFileReaders(Integer fileReaderThreads, Integer chunkSize,
			Integer fileSize, ChunkBuffer chunkBuffer) {
		List<ChunkFileReader> chunkFileReaders = new ArrayList<ChunkFileReader>();
		Integer chunksPerThread = fileSize / chunkSize / fileReaderThreads;
		for (int i = 0; i < fileReaderThreads; i++) {
			ChunkFileReader reader = new ChunkFileReader(chunkBuffer, "fileReader " + i,
					"/home/mrosenfeld/repo/tera-sort/dataset", i * chunkSize * chunksPerThread, chunksPerThread,
					chunkSize);
			chunkFileReaders.add(reader);
		}
		if (fileSize / chunkSize % fileReaderThreads > 0) {
			ChunkFileReader lastChunkFileReader = chunkFileReaders.get(chunkFileReaders.size() - 1);
			lastChunkFileReader
					.setChunkCount(lastChunkFileReader.getChunkCount() + (fileSize / chunkSize % fileReaderThreads));
		}
		return chunkFileReaders;
	}

}
