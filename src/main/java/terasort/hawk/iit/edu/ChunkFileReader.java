package terasort.hawk.iit.edu;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ChunkFileReader extends Thread {
	private ChunkBuffer chunkBuffer;
	private String filename;
	private Long offset;
	private Integer chunkCount;
	private Integer chunkSize;

	public ChunkFileReader(ChunkBuffer chunkBuffer, String threadName, String filename, Long offset, Integer chunkCount,
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
		// read assigned chunk from file and write in buffer
		InputStream inputStream = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		try {
			inputStream = new FileInputStream(this.filename);

			// create new input stream reader
			isr = new InputStreamReader(inputStream);
			br = new BufferedReader(isr);
			// seek until offset
			br.skip(offset);

			// creates buffer
			char[] cbuf = new char[chunkSize];

			for (int i = 0; i < chunkCount; i++) {
				br.read(cbuf, 0, chunkSize);
				chunkBuffer.write(String.valueOf(cbuf), this.getName());
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

	public Integer getChunkCount() {
		return chunkCount;
	}

	public void setChunkCount(Integer chunkCount) {
		this.chunkCount = chunkCount;
	}

}
