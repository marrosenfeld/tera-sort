package terasort.hawk.iit.edu;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author mrosenfeld Reads subchunks from file
 */
public class SubChunkFileReader extends Thread {
	private SubChunkBuffer subChunkBuffer;
	private String filename;
	private Long offset;
	private Integer subChunkCount;
	private Integer chunkSize;
	private Integer subChunkSize;

	public SubChunkFileReader(SubChunkBuffer subChunkBuffer, String filename, Long offset, Integer subChunkCount,
			Integer chunkSize, Integer subChunkSize) {
		super();
		this.subChunkBuffer = subChunkBuffer;
		this.filename = filename;
		this.offset = offset;
		this.subChunkCount = subChunkCount;
		this.chunkSize = chunkSize;
		this.subChunkSize = subChunkSize;

	}

	@Override
	public void run() {

		RandomAccessFile raf = null;
		// try {

		try {
			raf = new RandomAccessFile(filename, "rw");
			raf.seek(offset);

			Integer index = (int) (offset / chunkSize);
			// creates buffer
			byte[] cbuf = new byte[subChunkSize];

			for (int i = 0; i < subChunkCount; i++) {

				int read = raf.read(cbuf, 0, subChunkSize);

				Integer subChunkIndex = (int) ((offset % chunkSize) / subChunkSize);
				subChunkBuffer.write(subChunkIndex, index, new String(cbuf, "UTF8"), "2");

				index++;
				raf.skipBytes(chunkSize - subChunkSize);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				raf.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
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

	public Integer getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(Integer chunkSize) {
		this.chunkSize = chunkSize;
	}

	public Integer getSubChunkCount() {
		return subChunkCount;
	}

	public void setSubChunkCount(Integer subChunkCount) {
		this.subChunkCount = subChunkCount;
	}

}
