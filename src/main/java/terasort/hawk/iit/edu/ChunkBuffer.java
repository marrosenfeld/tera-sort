package terasort.hawk.iit.edu;

import java.util.ArrayList;

public class ChunkBuffer {
	private ArrayList<Record> buffer;
	private Integer capacity;

	public ChunkBuffer(Integer capacity) {
		super();
		this.capacity = capacity;
		buffer = new ArrayList<Record>();
	}

	public boolean isEmpty() {
		return buffer.isEmpty();
	}

	public boolean isFull() {
		return capacity.equals(buffer.size());
	}
	
	public synchronized void write(Record record, String writerName){
        while(this.isFull()){
           try {
              wait();
           } catch (InterruptedException e) {
              e.printStackTrace();
           }
        }
        buffer.add(record);
//        System.out.println("Data "+ record.getValue() +" produced by "+writerName);
        notifyAll();
     }
	
	public synchronized Record read(String readerName) throws InterruptedException{
        while(isEmpty()){
           try {
              wait();
           } catch (InterruptedException e) {
              e.printStackTrace();
           }
        }
        Record record = buffer.remove(0);
        Thread.sleep(1000);
        notifyAll();
//        System.out.println("Data "+ record.getValue() + " consumed by "+ readerName);
        return record;
     }
}
