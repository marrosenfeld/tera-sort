package terasort.hawk.iit.edu;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ChunkFileWriter extends Thread {
	private ChunkBuffer chunkBuffer;
	private String filename;
	private Long offset;
	private Integer chunkCount;
	private Integer chunkSize;

	public ChunkFileWriter(ChunkBuffer chunkBuffer, String threadName, String filename, Long offset, Integer chunkCount,
			Integer chunkSize) {
		this.chunkBuffer = chunkBuffer;
		this.setName(threadName);
		this.filename = filename;
		this.offset = offset;
		this.chunkCount = chunkCount;
		this.chunkSize = chunkSize;
	}

	@Override
	public void run() {

		RandomAccessFile raf = null;

		try {
			raf = new RandomAccessFile(filename, "rw");
			raf.seek(offset);
			for (int i = 0; i < chunkCount; i++) {
				String chunk = chunkBuffer.read(this.getName());
				if (i % 10 == 0) {
					System.out.println(
							String.format("%s write orderd chunk %d/%d to file", this.getName(), i + 1, chunkCount));
				}
				raf.writeBytes(chunk);
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

	public Long getOffset() {
		return offset;
	}

	public void setOffset(Long offset) {
		this.offset = offset;
	}

	public Integer getChunkCount() {
		return chunkCount;
	}

	public void setChunkCount(Integer chunkCount) {
		this.chunkCount = chunkCount;
	}

	public Integer getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(Integer chunkSize) {
		this.chunkSize = chunkSize;
	}

}
