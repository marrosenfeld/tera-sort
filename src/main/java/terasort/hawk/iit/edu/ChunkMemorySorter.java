package terasort.hawk.iit.edu;

public class ChunkMemorySorter extends Thread {
	private ChunkBuffer chunkBuffer;
    
    public ChunkMemorySorter(ChunkBuffer chunkBuffer, String threadName){
       this.chunkBuffer = chunkBuffer;
       setName(threadName);
    }
     
    @Override
    public void run() {
    	for (int i = 0; i < 6; i++) {
    		try {
				chunkBuffer.read(this.getName());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    	
//       while(true){
//          try {
//             Thread.sleep(500);
//          } catch (InterruptedException e) {
//             e.printStackTrace();
//          }
//          buffer.consume(getName());
//       }
    }
}
