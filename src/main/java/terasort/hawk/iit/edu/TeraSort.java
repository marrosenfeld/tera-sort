package terasort.hawk.iit.edu;

public class TeraSort {

	public static void main(String[] args) {
//		ProducerConsumerProblem pcp = new ProducerConsumerProblem();
	      ChunkBuffer chunkBuffer = new ChunkBuffer(1);
	       
	      ChunkFileReader reader = new ChunkFileReader(chunkBuffer, "fileReader", "dataset", 0, 3, 100);
	      ChunkMemorySorter sorter = new ChunkMemorySorter(chunkBuffer, "sorter");
	       
	      reader.start();
	      sorter.start();
	      
	      try {
	            reader.join();
	            sorter.join();
	        } catch (InterruptedException e) {
	            e.printStackTrace();
	        }
	      System.out.println("End");
	}

}
