package terasort.hawk.iit.edu;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FinalChunkFileWriter extends Thread {
	private ChunkBuffer chunkBuffer;
	private String filename;
	private Integer chunkSize;
	private Long fileSize;
	private String filePath;

	public FinalChunkFileWriter(ChunkBuffer chunkBuffer, String threadName, String filename, Integer chunkSize,
			Long fileSize, String filePath) {
		this.chunkBuffer = chunkBuffer;
		this.setName(threadName);
		this.filename = filename;
		this.filePath = filePath;
		this.chunkSize = chunkSize;
		this.fileSize = fileSize;
	}

	@Override
	public void run() {

		RandomAccessFile raf = null;

		try {
			raf = new RandomAccessFile(filePath + filename, "rw");
			Integer size = 0;
			while (size < fileSize) {

				String chunk = chunkBuffer.read(this.getName());
				// + (2 * i * chunkSize));
				raf.writeBytes(chunk);
				size += chunk.length();
				if (size % 100000000 == 0) {
					System.out.println(String.format("Write %d/%d to final file", size, fileSize));
				}
			}
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (raf != null)
					raf.close();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	public ChunkBuffer getChunkBuffer() {
		return chunkBuffer;
	}

	public void setChunkBuffer(ChunkBuffer chunkBuffer) {
		this.chunkBuffer = chunkBuffer;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public Integer getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(Integer chunkSize) {
		this.chunkSize = chunkSize;
	}

}
