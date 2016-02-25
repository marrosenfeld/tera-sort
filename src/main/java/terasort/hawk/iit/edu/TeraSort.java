package terasort.hawk.iit.edu;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TeraSort {

	public static void main(String[] args) {

		Integer fileThreads = Integer.valueOf(args[0]);
		Integer memoryThreads = Integer.valueOf(args[1]);
		// Chunk Size in Bytes
		Integer chunkSize = Integer.valueOf(args[2]);
		// Record size is 100
		Integer recordSize = 100;
		// File Size in bytes
		Long fileSize = Long.valueOf(args[3]);
		// Available memory in bytes
		Long availableMemory = Long.valueOf(args[4]);
		// files path
		String filePath = args[5];

		// Buffer size = number of chunks in buffer
		Integer bufferSize = ((Long) (availableMemory / 2 / chunkSize)).intValue();

		// declare buffers
		ChunkBuffer chunkBuffer = new ChunkBuffer(bufferSize);
		ChunkBuffer orderedChunkBuffer = new ChunkBuffer(bufferSize);

		removeCurrentFiles(filePath);
		// sort phase

		List<ChunkFileReader> chunkFileReaders = getChunkFileReaders(fileThreads / 2, chunkSize, fileSize, chunkBuffer,
				filePath);
		List<ChunkMemorySorter> chunkMemorySorters = getChunkMemorySorters(memoryThreads, fileSize, chunkSize,
				recordSize, chunkBuffer, orderedChunkBuffer);
		List<ChunkFileWriter> chunkFileWriters = getChunkFileWriters(fileThreads / 2, chunkSize, fileSize,
				orderedChunkBuffer, filePath);

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

		Merger merger = new Merger(fileSize, chunkSize, availableMemory, fileThreads, recordSize);
		merger.merge();
		// merge phase
		System.out.println("End");
	}

	private static void removeCurrentFiles(String filePath) {
		File file = new File(filePath + "dataset_tmp");
		file.delete();
		file = new File(filePath + "dataset_final");
		file.delete();
	}

	private static List<ChunkFileWriter> getChunkFileWriters(Integer fileWriterThreads, Integer chunkSize,
			Long fileSize, ChunkBuffer orderedChunkBuffer, String filePath) {
		List<ChunkFileWriter> chunkFileWriters = new ArrayList<ChunkFileWriter>();
		Integer chunksPerThread = ((Long) (fileSize / chunkSize / fileWriterThreads)).intValue();
		for (int i = 0; i < fileWriterThreads; i++) {
			ChunkFileWriter writer = new ChunkFileWriter(orderedChunkBuffer, "fileWriter " + i,
					filePath + "dataset_tmp", i * chunkSize * chunksPerThread, chunksPerThread, chunkSize);
			chunkFileWriters.add(writer);
		}
		if (fileSize / chunkSize % fileWriterThreads > 0) {
			ChunkFileWriter lastChunkFileWriter = chunkFileWriters.get(chunkFileWriters.size() - 1);
			lastChunkFileWriter.setChunkCount(
					((Long) (lastChunkFileWriter.getChunkCount() + (fileSize / chunkSize % fileWriterThreads)))
							.intValue());
		}
		return chunkFileWriters;

	}

	private static List<ChunkMemorySorter> getChunkMemorySorters(Integer memoryThreads, Long fileSize,
			Integer chunkSize, Integer recordSize, ChunkBuffer chunkBuffer, ChunkBuffer orderedChunkBuffer) {

		List<ChunkMemorySorter> chunkMemorySorters = new ArrayList<ChunkMemorySorter>();

		Integer chunksPerThread = ((Long) (fileSize / chunkSize / memoryThreads)).intValue();

		for (int i = 0; i < memoryThreads; i++) {
			ChunkMemorySorter sorter = new ChunkMemorySorter(chunkBuffer, "MemorySorter" + i, recordSize,
					chunksPerThread, chunkSize, orderedChunkBuffer);
			chunkMemorySorters.add(sorter);
		}

		if (fileSize / chunkSize % memoryThreads > 0) {
			ChunkMemorySorter lastChunkMemorySorter = chunkMemorySorters.get(chunkMemorySorters.size() - 1);

			lastChunkMemorySorter.setChunkCount(
					((Long) (lastChunkMemorySorter.getChunkCount() + (fileSize / chunkSize % memoryThreads)))
							.intValue());
		}

		return chunkMemorySorters;
	}

	private static List<ChunkFileReader> getChunkFileReaders(Integer fileThreads, Integer chunkSize, Long fileSize,
			ChunkBuffer chunkBuffer, String filePath) {
		List<ChunkFileReader> chunkFileReaders = new ArrayList<ChunkFileReader>();
		Integer chunksPerThread = ((Long) (fileSize / chunkSize / fileThreads)).intValue();
		for (int i = 0; i < fileThreads; i++) {
			ChunkFileReader reader = new ChunkFileReader(chunkBuffer, "fileReader " + i, filePath + "dataset",
					i * chunkSize * chunksPerThread, chunksPerThread, chunkSize);
			chunkFileReaders.add(reader);
		}
		if (fileSize / chunkSize % fileThreads > 0) {
			ChunkFileReader lastChunkFileReader = chunkFileReaders.get(chunkFileReaders.size() - 1);
			lastChunkFileReader.setChunkCount(
					((Long) (lastChunkFileReader.getChunkCount() + (fileSize / chunkSize % fileThreads))).intValue());
		}
		return chunkFileReaders;
	}

}
