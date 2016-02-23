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
		
		List<ChunkFileReader> chunkFileReaders = new ArrayList<ChunkFileReader>();
		List<ChunkMemorySorter> chunkMemorySorters = new ArrayList<ChunkMemorySorter>();
		
		ChunkBuffer chunkBuffer = new ChunkBuffer(2);

		for (int i = 0; i < fileReaderThreads; i++) {
			ChunkFileReader reader = new ChunkFileReader(chunkBuffer, "fileReader " + i,
					"/home/mrosenfeld/repo/tera-sort/dataset", i * chunkSize * 3, 3, chunkSize);
			chunkFileReaders.add(reader);
		}
		
		ChunkMemorySorter sorter = new ChunkMemorySorter(chunkBuffer, "sorter");
		
		for (ChunkFileReader chunkFileReader : chunkFileReaders) {
			chunkFileReader.start();
		}
		
		sorter.start();

		try {
			for (ChunkFileReader chunkFileReader : chunkFileReaders) {
				chunkFileReader.join();
			}
			sorter.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("End");
	}

}
