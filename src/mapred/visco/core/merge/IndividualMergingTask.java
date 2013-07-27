package visco.core.merge;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.concurrent.Executor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;

import visco.util.ActionDelegate;
import visco.util.ModifiableBoolean;

/**
 * This is the task created when we have only one incoming map stream. 
 * */
public class IndividualMergingTask<K extends WritableComparable<K>, V extends Writable> implements Runnable {

	
	private class State {
		
		/**
		 * The task is not blocked
		 */
		private final static int NotBlocked = 0;

		/**
		 * The task is blocked on the rx channel #0
		 */
		private final static int BlockedOnRX_0 = 1 << 0;
	}

	/**
	 * The rx channel #0
	 */
	private IOChannel<IOChannelBuffer<K, V>> rxChannel_0;

	/**
	 * The tx channel
	 */
	private IOChannel<IOChannelBuffer<K, V>> txChannel;

	/**
	 * The buffer from rx channel #0
	 */
	private IOChannelBuffer<K, V> rxBuffer_0 = null;
	
	/**
	 * The lock for this task
	 */
	private Object taskLock = new Object();

	/**
	 * Is the task running
	 */
	private boolean taskIsRunning = false;

	/**
	 * The task state. At the beginning we are blocked on every channel
	 */
	private int taskState = State.BlockedOnRX_0;

	/**
	 * This is the thread pool that we use to schedule this task.
	 */
	protected final Executor threadPool;

	/**
	 * The reporter for the job
	 */
	private Reporter reporter;

	/**
	 * We used a static int to keep track of how many tasks we generate.
	 */
	public static int indexCounter = 0;

	/**
	 * The task index (useful for debug)
	 */
	private final int index;
	
	private final JobConf job;
	
	private final ActionDelegate onMergeCompleted;
	
	/////////////////////////////////////		NEW API

	/**
	 * A reducer to reduce the final output data
	 */
	private org.apache.hadoop.mapreduce.Reducer<K,V,K,V> newReducer;
	
	/**
	 * A task context to get the classes
	 */
	private TaskAttemptContext taskContext;
	
	/**
	 * A context to hold the reducer
	 */
	private org.apache.hadoop.mapreduce.Reducer<K,V,K,V>.Context reducerContext;

	private NewTrackingRecordWriter<K, V> newWriter;
	
	/**
	 * The output format of the job
	 */
	private OutputFormat<?,?> outputFormat;
	
	/////////////////////////////////////		OLD API

	/**
	 * A reducer to reduce the final output data
	 */
	private org.apache.hadoop.mapred.Reducer<K,V,K,V> oldReducer;
	
	/**
	 * A record writer to write the final output
	 */
	private OldTrackingRecordWriter<K,V> oldTrackedRW;
	
	/**
	 * The collector to collect the output
	 * */
	private OutputCollector<K, V> oldCollector;
	
	/**
	 * Initializes an empty merger task. The channels must be initialized (using
	 * {@link MergeTask#SetRXChannels(IIoChannel, IIoChannel)} and
	 * {@link MergeTask#SetTXChannel(IIoChannel)}
	 * 
	 * @param threadPool
	 *            the thread pool used to execute this task
	 * 
	 */
	IndividualMergingTask(Executor threadPool, Reporter reporter, JobConf jobConf, String finalName,
			Counter keyCounter, Counter valueCounter, Counter outputCounter, TaskAttemptID taskId, 
			ActionDelegate onMergeCompleted) {
		
		this.job = jobConf;
		this.threadPool = threadPool;
		this.reporter = reporter;
		this.index = MergingTask.indexCounter++;
		this.onMergeCompleted = onMergeCompleted;
		
		try {
			if(this.job.getUseNewReducer()) {
				this.taskContext = new TaskAttemptContext(jobConf, taskId);
				this.outputFormat = ReflectionUtils.newInstance(this.taskContext.getOutputFormatClass(), this.job);
				this.newReducer = (org.apache.hadoop.mapreduce.Reducer<K,V,K,V>)
						ReflectionUtils.newInstance(taskContext.getReducerClass(), jobConf);
				this.newWriter = new NewTrackingRecordWriter<K,V>(jobConf, reporter, taskContext, outputCounter, this.outputFormat);
				this.reducerContext = createReduceContext(this.newReducer, taskContext.getConfiguration(), taskId, 
						keyCounter, valueCounter, this.newWriter, outputFormat.getOutputCommitter(taskContext), (StatusReporter) reporter);
			} else {
				this.oldReducer = ReflectionUtils.newInstance(this.job.getReducerClass(), this.job);
				this.oldTrackedRW = new OldTrackingRecordWriter<K, V>((org.apache.hadoop.mapred.Counters.Counter) outputCounter, this.job, reporter, finalName);
				this.oldCollector = new OutputCollector<K, V>() {
					public void collect(K key, V value) throws IOException {
						oldTrackedRW.write(key, value);
					}
				};
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @return the index of this merging task.
	 * */
	public int getMergingTaskId() {
		return this.index;
	}
	
	/**
	 * Sets the rx channels.
	 * 
	 * @param rxChannel_0
	 * @param rxChannel_1
	 */
	void SetRXChannel(IOChannel<IOChannelBuffer<K, V>> rxChannel_0) {
		assert this.rxChannel_0 == null;
		this.rxChannel_0 = rxChannel_0;
	}

	public IOChannel<IOChannelBuffer<K,V>> getRXChannel() { 
		return this.rxChannel_0;
	}

	/**
	 * Channel RX 0 has some new data
	 */
	void UnblockRX_0() {
		boolean queueThread = false;
		synchronized (taskLock) {
			if (!taskIsRunning && (taskState & State.BlockedOnRX_0) != 0) {
				taskIsRunning = true;
				queueThread = true;
			}
		}
		if (queueThread)
			threadPool.execute(this);
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		assert rxChannel_0 != null : "rxChannel_0 is null";
		assert txChannel != null : "txChannel is null";

		ModifiableBoolean result = new ModifiableBoolean();

		while (true) {
			synchronized (taskLock) {
								
				if ((taskState & State.BlockedOnRX_0) != 0) {
					rxBuffer_0 = rxChannel_0.Receive(result);
					if (!result.value) {
						taskIsRunning = false;
						reporter.progress();
						return;
					}
					// remove the lock
					taskState = State.NotBlocked;
				}
			}

			if (rxBuffer_0 == null) {
				try {
					if(this.job.getUseNewReducer()) {
						this.newWriter.close(this.taskContext);
					} else {
						this.oldReducer.close();
						this.oldTrackedRW.close(this.reporter);
					}
					this.onMergeCompleted.action();
					
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			
				// indicate progress before we go to sleep
				reporter.progress();
				return;
			}

			while (taskState == State.NotBlocked) {
				if (rxBuffer_0 != null) {
					try {
						
						if(this.job.getUseNewReducer()) {
							while(this.rxBuffer_0.size() > 0) {
								K key = this.rxBuffer_0.removeKey();
								ArrayList<V> values = this.rxBuffer_0.removeValues();
								
								this.newReducer.reduce(key, values, this.reducerContext);
								reporter.progress();
							}
						} else {
							while(this.rxBuffer_0.size() > 0) {
								K key = this.rxBuffer_0.removeKey();
								ArrayList<V> values = this.rxBuffer_0.removeValues();
								
								this.oldReducer.reduce(key, values.iterator(), this.oldCollector, this.reporter);
								reporter.progress();
							}
						}
						this.rxBuffer_0.clear();
						this.rxChannel_0.Release(rxBuffer_0);
						this.rxBuffer_0 = null;
						
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					taskState = State.BlockedOnRX_0;
				}
				reporter.progress(); // indicate we are making progress
			}
		}
	}
	
	private static final Constructor<org.apache.hadoop.mapreduce.Reducer.Context> contextConstructor;
	
	static {
		try {
			contextConstructor = 
					org.apache.hadoop.mapreduce.Reducer.Context.class.getConstructor
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
	private org.apache.hadoop.mapreduce.Reducer<K,V,K,V>.Context createReduceContext(
			org.apache.hadoop.mapreduce.Reducer<K,V,K,V> reducer,
			Configuration job,
			TaskAttemptID taskId, 
			Counter keyCounter,
			Counter valueCounter,
			org.apache.hadoop.mapreduce.RecordWriter<K,V> output, 
			OutputCommitter committer,
			StatusReporter reporter
			) throws IOException, ClassNotFoundException {
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
	