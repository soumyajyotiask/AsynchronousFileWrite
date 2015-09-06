package asyn.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class AsyncFileWriterV2 {

	private final static String NEW_LINE = "\n";
	private final AsynchronousFileChannel channel;
	private final AtomicLong position;
	private final ConcurrentLinkedDeque<Future<Integer>> results;
	private final Path filePath;
	
	public AsyncFileWriterV2(String filePath) throws IOException {
		this.filePath = Paths.get(filePath);
		Files.deleteIfExists(this.filePath);
		Files.createFile(this.filePath);
		this.channel = AsynchronousFileChannel.open(this.filePath, StandardOpenOption.WRITE);
		position = new AtomicLong(0);
		results = new ConcurrentLinkedDeque<>();
	}
	
	public void writeLine(final String line){
		byte[] lineByte = (line+NEW_LINE).getBytes();
		long pos = position.getAndAdd(lineByte.length);
		results.add(channel.write(ByteBuffer.wrap(lineByte), pos));
	}
	
	public void writeLine(final List<String> lines) {
		this.writeLine(lines.stream().collect(Collectors.joining(NEW_LINE)));
	}

	synchronized public void close() throws Exception{
		System.out.println("Numbers of results to verify:"+results.size());
		results.parallelStream().forEach(result -> {
			try {
				result.get();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
		channel.close();
	}

}
