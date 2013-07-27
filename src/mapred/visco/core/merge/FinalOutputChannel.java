package visco.core.merge;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;

import visco.util.ActionDelegate;
import visco.util.ModifiableBoolean;

public class FinalOutputChannel<INKEY extends WritableComparable<INKEY>, INVALUE extends Writable, 
		OUTKEY extends WritableComparable<OUTKEY>, OUTVALUE extends Writable> implements IOChannel<IOChannelBuffer<INKEY,INVALUE>> {

	/**
	 * The configuration of the job
	 */
	private JobConf jobConf;

	/**
	 * A reporter for the progress
	 */
	private final Reporter reporter;

	/**
	 * The callback to execute when the merge process completes
	 */
	private final ActionDelegate onMergeCompleted;

	/**
	 * The IO channel buffer that we pass around.
	 */
	private final IOChannelBuffer<INKEY, INVALUE> ioChannelBuffer;

	
	///////////		NEW API
	/**
	 * The output format of the job
	 */
	private org.apache.hadoop.mapreduce.OutputFormat<OUTKEY, OUTVALUE> outputFormat;

	/**
	 * A reducer to reduce the final output data
	 */
	private org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> newReducer;

	/**
	 * A context to hold the reducer
	 */
	private org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>.Context reducerContext;
	
	/**
	 * A record writer to write the final output
	 */
	private NewTrackingRecordWriter<OUTKEY,OUTVALUE> newTrackedRW;
		
	/**
	 * A task context to get the classes
	 */
	private TaskAttemptContext taskContext;
	
	///////////		OLD API
	
	/**
	 * A reducer to reduce the final output data
	 */
	private org.apache.hadoop.mapred.Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> oldReducer;
	
	/**
	 * A record writer to write the final output
	 */
	private RecordWriter<OUTKEY, OUTVALUE> oldTrackedRW;
	
	/**
	 * The collector to collect the output
	 * */
	private org.apache.hadoop.mapred.OutputCollector<OUTKEY, OUTVALUE> oldCollector;
	
	private RawComparator<INKEY> groupComparator;
	
	/**
	 * The main constructor of the class.
	 * @param jobConf
	 * 				the configuration of the job
	 * @param reporter
	 * 				a reporter for the progress
	 * @param onMergeCompleted
	 * 				the callback to execute when the merge process completes
	 */
	@SuppressWarnings("unchecked")
	public FinalOutputChannel(JobConf jobConf, final Reporter reporter, Counter keyCounter, Counter valueCounter, 
			Counter outputCounter, TaskAttemptID taskId, String finalName, ActionDelegate onMergeCompleted) {
		
		this.jobConf = jobConf;
		this.ioChannelBuffer = new IOChannelBuffer<INKEY, INVALUE>(100, this.jobConf);
		this.reporter = reporter;
		this.onMergeCompleted = onMergeCompleted;
		
		// this is for the secondary sort.
		this.groupComparator = this.jobConf.getOutputValueGroupingComparator();
		
		try {
			if(this.jobConf.getUseNewReducer()) {
				this.taskContext = new TaskAttemptContext(jobConf, taskId);
				this.outputFormat = (OutputFormat<OUTKEY, OUTVALUE>) ReflectionUtils.newInstance(taskContext.getOutputFormatClass(), this.jobConf);
				this.newReducer = (Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>) ReflectionUtils.newInstance(taskContext.getReducerClass(), jobConf);
				this.newTrackedRW = new NewTrackingRecordWriter<OUTKEY, OUTVALUE>(jobConf, reporter, taskContext, outputCounter, outputFormat);
				this.reducerContext = createReduceContext(this.newReducer, taskContext.getConfiguration(), taskId, 
						keyCounter, valueCounter, this.newTrackedRW, outputFormat.getOutputCommitter(taskContext), (StatusReporter) reporter);
			} else {
				this.oldReducer = (org.apache.hadoop.mapred.Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>) 
						ReflectionUtils.newInstance(this.jobConf.getReducerClass(), this.jobConf);
				
				this.oldTrackedRW = new OldTrackingRecordWriter<OUTKEY, OUTVALUE>(
						(org.apache.hadoop.mapred.Counters.Counter) outputCounter, this.jobConf, this.reporter, finalName);
				
				this.oldCollector = new OutputCollector<OUTKEY, OUTVALUE>() {

					@Override
					public void collect(OUTKEY key, OUTVALUE value) throws IOException {
						oldTrackedRW.write(key, value);
						reporter.progress();
					}
				};
			}

		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}
	
	public IOChannelBuffer<INKEY,INVALUE> GetEmpty(ModifiableBoolean result) {
		return ioChannelBuffer;
	}
	
	private INKEY groupKey;
	private ArrayList<INVALUE> groupValues;
	
	public void Send(IOChannelBuffer<INKEY,INVALUE> item) {
		
		try {				
			while (item.size() > 0) {
				INKEY key = item.removeKey();
				ArrayList<INVALUE> values = item.removeValues();
					
				boolean areKeysEqual = (this.groupKey == null) ? false : 
					(this.groupComparator.compare(key, this.groupKey) == 0);

				if(areKeysEqual) {
					this.groupValues.addAll(values);
				} else {
					if(this.groupKey != null)
						this.runReducer(this.groupKey, this.groupValues); 
					this.groupKey = key;
					this.groupValues = values;
				}
				this.reporter.progress();	
			}
			item.clear(); // clear the buffer for the next iteration
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}

	public IOChannelBuffer<INKEY,INVALUE> Receive(ModifiableBoolean result) {
		throw new UnsupportedOperationException("This method should never be called");
	}

	public void Release(IOChannelBuffer<INKEY,INVALUE> item) {
		throw new UnsupportedOperationException("This method should never be called");
	}

	public void Close() {
		try {
			this.runReducer(groupKey, groupValues);
			if(this.jobConf.getUseNewReducer()) {
				this.newTrackedRW.close(this.taskContext);
			} else {
				this.oldReducer.close();
				this.oldTrackedRW.close(this.reporter);
			}
			this.ioChannelBuffer.clear();
			onMergeCompleted.action();
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}

	private final void runReducer(INKEY key, ArrayList<INVALUE> values) throws IOException, InterruptedException {
		if(this.jobConf.getUseNewReducer()) {
			this.newReducer.reduce(key, values, this.reducerContext); 
		} else {
			this.oldReducer.reduce(key, values.iterator(), this.oldCollector, this.reporter); 
		}
	}
	
	private static final Constructor<org.apache.hadoop.mapreduce.Reducer.Context> contextConstructor;
	
	static {
		try {
			contextConstructor = org.apache.hadoop.mapreduce.Reducer.Context.class.getConstructor
					(new Class[]{org.apache.hadoop.mapreduce.Reducer.class,
							Configuration.class,
							TaskAttemptID.class,
							org.apache.hadoop.mapreduce.RecordWriter.class,
							Counter.class,
							Counter.class,
							OutputCommitter.class,
							StatusReporter.class});
		} catch (NoSuchMethodException nme) {
			throw new IllegalArgumentException("Can't find constructor");
		}
	}

	@SuppressWarnings("unchecked")
	private org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>.Context createReduceContext(
			org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer,
			Configuration job,
			TaskAttemptID taskId, 
			Counter keyCounter,
			Counter valueCounter,
			org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE> output, 
			OutputCommitter committer,
			StatusReporter reporter) throws IOException, ClassNotFoundException {
		try {

			return contextConstructor.newInstance(reducer, job, taskId,
					output, keyCounter, valueCounter, committer, reporter);
		} catch (InstantiationException e) {
			throw new IOException("Can't create Context", e);
		} catch (InvocationTargetException e) {
			throw new IOException("Can't invoke Context constructor", e);
		} catch (IllegalAccessException e) {
			throw new IOException("Can't invoke Context constructor", e);
		}
	}
}
