package visco.core.merge;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.ReduceTask;
import org.apache.hadoop.mapred.Reporter;

public class OldTrackingRecordWriter<K1 extends WritableComparable<K1>,V1 extends Writable> implements RecordWriter<K1, V1> {

	private final RecordWriter<K1, V1> real;
	private final Counter outputRecordCounter;
	private final Counter fileOutputByteCounter;
	private final Statistics fsStats;

	private int recordCounter = 0;
	
	public OldTrackingRecordWriter(Counter outputRecordCounter, JobConf job, Reporter reporter, String finalName) throws IOException {
		
		this.outputRecordCounter = outputRecordCounter;
		this.fileOutputByteCounter = reporter.getCounter(FileOutputFormat.Counter.BYTES_WRITTEN);
		
		Statistics matchedStats = null;
		if (job.getOutputFormat() instanceof FileOutputFormat) {
			matchedStats = ReduceTask.getFsStatistics(FileOutputFormat.getOutputPath(job), job);
		}
		fsStats = matchedStats;

		FileSystem fs = FileSystem.get(job);
		long bytesOutPrev = getOutputBytes(fsStats);
		this.real = job.getOutputFormat().getRecordWriter(fs, job, finalName, reporter);
		long bytesOutCurr = getOutputBytes(fsStats);
		fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
	}

	@Override
	public void write(K1 key, V1 value) throws IOException {
		this.recordCounter++;
		
		long bytesOutPrev = getOutputBytes(fsStats);
		real.write(key, value);
		long bytesOutCurr = getOutputBytes(fsStats);
		fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
		outputRecordCounter.increment(1);
	}

	@Override
	public void close(Reporter reporter) throws IOException {
		long bytesOutPrev = getOutputBytes(fsStats);
		real.close(reporter);
		long bytesOutCurr = getOutputBytes(fsStats);
		fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
	}

	private long getOutputBytes(Statistics stats) {
		return stats == null ? 0 : stats.getBytesWritten();
	}
	
	public int getNoOfRecordsWritten() {
		return this.recordCounter;
	}
}
