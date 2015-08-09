package asyn.file;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.collect.Lists;

public class AsyncFileWriterTest {
	

	@Test
	public void testSingleLineWriter() throws Exception {
		List<Long> sourceContent = Lists.newArrayList();

		for(long i = 0; i < 1000000; i++)
			sourceContent.add(i);
		
		System.out.println("Application started...");
		AsyncFileWriter writer = new AsyncFileWriter(3, "C:/application", "Test.dat");
		System.out.println("Starting writing...");
		long start = System.currentTimeMillis();
		sourceContent.parallelStream().forEach(line -> writer.writeLine(""+line));
		writer.close();
		long end = System.currentTimeMillis();
		System.out.println("Time took in Sec:"+(end-start)/1000);
		assertTrue(true);
	}

	@Test
	public void testMultipleLineWriter() throws Exception {
		System.out.println("Application started...");
		AsyncFileWriter writer = new AsyncFileWriter(3, "C:/application", "TestMultiLine.dat");
		ArrayList<ExecutableTask> taskList = Lists.newArrayList();
		long batchSize = 1000000;
		
		for(int taskId = 0; taskId < 10 ; taskId++ ){
			long start= taskId * batchSize;
			long end = start+batchSize;
			taskList.add(new ExecutableTask(start, end, writer));
		}

		ExecutorService executor = Executors.newFixedThreadPool(12);
		System.out.println("Starting writing...");
		long start = System.currentTimeMillis();
		executor.invokeAll(taskList);
		
		writer.close();
		long end = System.currentTimeMillis();
		System.out.println("Time took in Sec:"+(end-start)/1000);
		assertTrue(true);
	}
	
	private class ExecutableTask implements Callable<Void>{

		private final List<String> records;
		private final AsyncFileWriter writer;
		
		public ExecutableTask(long start, long end, AsyncFileWriter writer) {
			super();
			this.writer = writer;
			this.records = Lists.newArrayList();
			for(long i = start; i < end; i++){
				records.add(""+i);
			}
		}

		@Override
		public Void call() throws Exception {
			writer.writeLine(records);
			return null;
		}
		
	}
}
