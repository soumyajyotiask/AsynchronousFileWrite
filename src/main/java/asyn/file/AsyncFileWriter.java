package asyn.file;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class AsyncFileWriter {

	private final static String separator = "/";
	private static final String THREAD_NAME = "fileWriterThread-";
	private static final String MAIN_THREAD_NAME = "fileWriterThread-0";
	private final ExecutorService executor;
	private final ImmutableMap<String, BufferedWriter> threadFileWriter;
	private final String folderPath;
	private final String fileName;
	private final Supplier<ImmutableMap<String,Path>> outputFilePath;
	
	public AsyncFileWriter(int concurrencyLevel, String folderPath,
			String fileName) {
		this.folderPath = folderPath;
		this.fileName = fileName;
		executor = Executors.newFixedThreadPool(concurrencyLevel,
				new ThreadFactoryBuilder().setNameFormat("fileWriterThread-%d")
						.build());
		outputFilePath = () -> {
			ImmutableMap.Builder<String, Path> builder = new ImmutableMap.Builder<>();
			for (int i = 0; i < concurrencyLevel; i++) {
				String filePath = folderPath + separator + fileName
						+ (i == 0 ? "" : i);
				builder.put(THREAD_NAME+ i, Paths.get(filePath));
			}
			return builder.build();
		};
		ImmutableMap.Builder<String, BufferedWriter> builder = new ImmutableMap.Builder<>();
		outputFilePath.get().forEach(
						(threadName, path) -> {
							try {
								Files.deleteIfExists(path);
								builder.put(threadName, Files.newBufferedWriter(path,StandardOpenOption.CREATE,StandardOpenOption.APPEND));
							} catch (Exception e) {
								e.printStackTrace();
							}
						});

		threadFileWriter = builder.build();
	}
	
	public void writeLine(final String line){
		executor.submit(()->{
			String threadName = Thread.currentThread().getName();
			try {
				BufferedWriter bw = threadFileWriter.get(threadName);
				bw.append(line);
				bw.newLine();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		});
	}
	
	public void writeLine(final List<String> lines) {
		lines.forEach(line -> this.writeLine(line));
	}

	synchronized public void close() throws InterruptedException, IOException{
		if(executor.isTerminated())
			return;
		
		System.out.println("Turning the worker down...");
		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		
		System.out.println("Closing buffers...");
		threadFileWriter.values().forEach(bw -> {
			try {
				bw.flush();
				bw.close();
			} catch (Exception e) {
				throw new RuntimeException("Not able to close file resorces",e);
				//TODO Recovery plan.
			}
		});
		
		ImmutableMap<String,Path> paths = outputFilePath.get();
		if (paths.size() > 1) {
			System.out.println("Mering the files..");
			try (FileChannel dest = new FileOutputStream(paths.get(MAIN_THREAD_NAME)
					.toFile(),true).getChannel();) {
				dest.position(dest.size());

				for (Map.Entry<String, Path> entry : paths.entrySet()) {
					if (!MAIN_THREAD_NAME.equals(entry.getKey())) {
						try (FileChannel src = new FileInputStream(entry
								.getValue().toFile()).getChannel();) {
							src.transferTo(0, src.size(), dest);
						} catch (Exception e) {
							throw new RuntimeException(
									"Not able to write in main file", e);
							// TODO Recovery plan.
						}
						Files.deleteIfExists(entry.getValue());
					}
				}
			} catch (IOException e) {
				throw new RuntimeException("Not able to write in main file", e);
				// TODO Recovery plan.
			}
		}
		deleteLastLine(paths.get(MAIN_THREAD_NAME));
	}
	
	private void deleteLastLine(Path path) throws IOException{
		RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw");
		long length = file.length()-2;
		file.setLength(length);
		file.close();
	}
	
	public String getFolderPath() {
		return folderPath;
	}

	public String getFileName() {
		return fileName;
	}
}
