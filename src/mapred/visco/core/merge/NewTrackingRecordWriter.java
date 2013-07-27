package visco.core.merge;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ReduceTask;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NewTrackingRecordWriter<K1 extends WritableComparable<K1>,V1 extends Writable> extends RecordWriter<K1,V1> {

	private final RecordWriter<K1,V1> real;

	private final Counter fileOutputByteCounter;
	private final Counter outputRecordCounter;

	private final Statistics fsStats;

	@SuppressWarnings("unchecked")
	public NewTrackingRecordWriter(JobConf conf, Reporter reporter, TaskAttemptContext taskContext, 
			Counter outputRecordCounter, OutputFormat outputFormat) throws ClassNotFoundException, IOException, InterruptedException {

		this.fileOutputByteCounter = reporter.getCounter(FileOutputFormat.Counter.BYTES_WRITTEN);
		this.outputRecordCounter = outputRecordCounter;

		Statistics matchedStats = null;
		if (outputFormat instanceof org.apache.hadoop.mapreduce.lib.output.FileOutputFormat) {
			matchedStats = ReduceTask.getFsStatistics(FileOutputFormat.getOutputPath(taskContext), conf);
		}
		fsStats = matchedStats;

		long bytesOutPrev = getOutputBytes(fsStats);
		this.real = (RecordWriter<K1, V1>) outputFormat.getRecordWriter(taskContext);
		long bytesOutCurr = getOutputBytes(fsStats);
		
		this.fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);			
	}

	@Override
	public void write(K1 key, V1 value) throws IOException, InterruptedException {
		long bytesOutPrev = getOutputBytes(fsStats);
		real.write(key,value);
		long bytesOutCurr = getOutputBytes(fsStats);
		fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
		outputRecordCounter.increment(1);
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		long bytesOutPrev = getOutputBytes(fsStats);
		real.close(context);
		long bytesOutCurr = getOutputBytes(fsStats);
		fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
	}	

	private long getOutputBytes(Statistics stats) {
		return stats == null ? 0 : stats.getBytesWritten();
	}
}