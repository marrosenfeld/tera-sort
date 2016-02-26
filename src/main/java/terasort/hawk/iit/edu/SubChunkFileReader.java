package terasort.hawk.iit.edu;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class SubChunkFileReader extends Thread {
	private SubChunkBuffer subChunkBuffer;
	private String filename;
	private Integer offset;
	private Integer subChunkCount;
	private Integer chunkSize;
	private Integer subChunkSize;

	public SubChunkFileReader(SubChunkBuffer subChunkBuffer, String filename, Integer offset, Integer subChunkCount,
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
		// read assigned chunk from file and write in buffer
		InputStream inputStream = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		try {
			inputStream = new FileInputStream(this.filename);

			// create new input stream reader
			isr = new InputStreamReader(inputStream);
			br = new BufferedReader(isr);

			br.skip(offset);
			Integer index = offset / chunkSize;
			// creates buffer
			char[] cbuf = new char[Math.min(subChunkSize, ((index + 1) * chunkSize) - offset)];

			for (int i = 0; i < subChunkCount; i++) {
				int read = br.read(cbuf, 0, Math.min(subChunkSize, ((index + 1) * chunkSize) - offset));
				subChunkBuffer.write(index, String.valueOf(cbuf), this.getName());
				// System.out.println(String.format("SubChunk read %d-%d",
				// index,
				// subChunkBuffer.getBuffer()[index].getSubChunkIndex()));
				index++;
				br.skip(chunkSize - subChunkSize);
			}
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (inputStream != null)
					inputStream.close();
				if (isr != null)
					isr.close();
				if (br != null)
					br.close();
			} catch (Exception e) {
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

	public Integer getOffset() {
		return offset;
	}

	public void setOffset(Integer offset) {
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
