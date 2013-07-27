package visco.core.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Executor;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task.CombinerRunner;

import visco.util.ModifiableBoolean;

public class MergingTask <K extends WritableComparable<K>, V extends Writable> implements Runnable {

	
	private class State {
		
		/**
		 * The task is not blocked
		 */
		private final static int NotBlocked = 0;

		/**
		 * The task is blocked on the rx channel #0
		 */
		private final static int BlockedOnRX_0 = 1 << 0;

		/**
		 * The task is blocked on the rx channel #1
		 */
		private final static int BlockedOnRX_1 = 1 << 1;

		/**
		 * The task is blocked on the tx channel
		 */
		private final static int BlockedOnTX = 1 << 2;

		/**
		 * The task is blocked on both rx channels.
		 */
		private final static int BlockedOnAllRX = BlockedOnRX_0 | BlockedOnRX_1;

		/**
		 * The task is blocked on all channel
		 */
		private final static int BlockedOnAll = BlockedOnAllRX | BlockedOnTX;
	}

	/**
	 * The rx channel #0
	 */
	private IOChannel<IOChannelBuffer<K, V>> rxChannel_0;

	/**
	 * The rx channel #1
	 */
	private IOChannel<IOChannelBuffer<K, V>> rxChannel_1;

	/**
	 * The tx channel
	 */
	private IOChannel<IOChannelBuffer<K, V>> txChannel;

	/**
	 * The buffer from rx channel #0
	 */
	private IOChannelBuffer<K, V> rxBuffer_0 = null;

	/**
	 * The buffer from rx channel #1
	 */
	private IOChannelBuffer<K, V> rxBuffer_1 = null;

	/**
	 * The buffer for tx channel
	 */
	private IOChannelBuffer<K, V> txBuffer = null;
	
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
	private int taskState = State.BlockedOnAll;

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

	private final JobConf jobConf;
	
	// TODO think if this should be instantiated in each class separately
	private final CombinerRunner<K,V> combinerRunner;
	
	private final RawComparator comparator;
	
	/**
	 * Initializes an empty merger task. The channels must be initialized (using
	 * {@link MergeTask#SetRXChannels(IIoChannel, IIoChannel)} and
	 * {@link MergeTask#SetTXChannel(IIoChannel)}
	 * 
	 * @param threadPool
	 *            the thread pool used to execute this task
	 * 
	 */
	MergingTask(JobConf jobConf, Executor threadPool, CombinerRunner combiner, Reporter reporter) {
		this.threadPool = threadPool;
		this.reporter = reporter;
		this.combinerRunner = combiner;
		this.index = MergingTask.indexCounter++;
		
		this.jobConf = jobConf;
		this.comparator = this.jobConf.getOutputKeyComparator();//.getOutputValueGroupingComparator();
	}

	/**
	 * Initializes a new task with the proper channels.
	 * 
	 * @param rxChannel_0
	 * @param rxChannel_1
	 * @param txChannel
	 * 
	 * @param threadPool
	 *            the thread pool used to execute this task
	 */
	public MergingTask(JobConf jobConf, IOChannel<IOChannelBuffer<K, V>> rxChannel_0,
			IOChannel<IOChannelBuffer<K, V>> rxChannel_1,
			IOChannel<IOChannelBuffer<K, V>> txChannel, 
			Executor threadPool, CombinerRunner combiner, Reporter reporter) {
		this.rxChannel_0 = rxChannel_0;
		this.rxChannel_1 = rxChannel_1;
		this.txChannel = txChannel;
		this.threadPool = threadPool;
		this.reporter = reporter;
		
		this.jobConf = jobConf;
		this.comparator = this.jobConf.getOutputKeyComparator();//.getOutputValueGroupingComparator();
		
		this.combinerRunner = combiner;
		
		this.index = MergingTask.indexCounter++;
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
	void SetRXChannels(IOChannel<IOChannelBuffer<K, V>> rxChannel_0,
			IOChannel<IOChannelBuffer<K, V>> rxChannel_1) {

		assert this.rxChannel_0 == null && this.rxChannel_1 == null;
		this.rxChannel_0 = rxChannel_0;
		this.rxChannel_1 = rxChannel_1;
	}

	/**
	 * Set the tx channel
	 * 
	 * @param txChannel
	 */
	void SetTXChannel(IOChannel<IOChannelBuffer<K, V>> txChannel) {
		assert this.txChannel == null;
		this.txChannel = txChannel;
	}

	public IOChannel<IOChannelBuffer<K,V>> getTXChannel() { 
		return this.txChannel; 
	}

	public IOChannel<IOChannelBuffer<K,V>> getRXChannel(int branch) { 
		return (branch == 0) ? this.rxChannel_0 : this.rxChannel_1;
	}

	/**
	 * Channel RX 0 has some new data
	 */
	void UnblockRX_0() {
		boolean queueThread = false;
		synchronized (taskLock) {
			if (!taskIsRunning && (taskState & State.BlockedOnRX_0) != 0) {
				// start the task
				taskIsRunning = true;
				queueThread = true;
			}
		}

		if (queueThread)
			// queue the task
			threadPool.execute(this);
	}

	/**
	 * Channel RX 1 has some new data
	 */
	void UnblockRX_1() {
		boolean queueThread = false;
		synchronized (taskLock) {
			
			// wake up only if the task is not running and if the task is blocked on RX_1 but not RX_0
			if (!taskIsRunning && (taskState & State.BlockedOnAllRX) == State.BlockedOnRX_1) {
				// start the task
				taskIsRunning = true;
				queueThread = true;
			}
		}

		if (queueThread)
			// queue the task
			threadPool.execute(this);
	}

	void UnblockTX() {
		boolean queueThread = false;
		synchronized (taskLock) {
			/*
			 * wake up only if the task is not running and if the task is bloced
			 * on TX but neither on RX_0 nor RX_1
			 */
			if (!taskIsRunning && taskState == State.BlockedOnTX) {
				// start the task
				taskIsRunning = true;
				queueThread = true;
			}
		}

		if (queueThread)
			// queue the task
			threadPool.execute(this);
	}

	private int zeroValuesRead = 0;
	private int oneValuesRead = 0;
	
	private int noOfRecordsIn = 0;
	private int noOfRecordsOut = 0;
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		assert rxChannel_0 != null : "rxChannel_0 is null";
		assert rxChannel_1 != null : "rxChannel_1 is null";
		assert txChannel != null : "txChannel is null";

		int compare;
		ModifiableBoolean result = new ModifiableBoolean();

		while (true) {
			
			synchronized (taskLock) {
				
				// these ifs just fix the state flags and take new buffers (input and output)
				if ((taskState & State.BlockedOnRX_0) != 0) {
					
					rxBuffer_0 = rxChannel_0.Receive(result);
					if (!result.value) {

						// we are going to sleep => send the current buffer (if any)
						if (txBuffer != null) {
							
//							if(this.combinerRunner != null) // && values_0.size() >= 4) {
//								this.runCombinerOnBuffer(this.txBuffer);
							

							this.noOfRecordsOut += txBuffer.size();
							txChannel.Send(txBuffer);
							
							txBuffer = null;
							taskState |= State.BlockedOnTX;
						}
						taskIsRunning = false;

						// indicate progress before we go to sleep
						reporter.progress();
						return;
					}
					// remove the block on rx 0
					taskState &= ~State.BlockedOnRX_0;
				}

				if ((taskState & State.BlockedOnRX_1) != 0) {					
					
					rxBuffer_1 = rxChannel_1.Receive(result);
					if (!result.value) {
						
						// we are going to sleep => send the current buffer (if any)
						if (txBuffer != null) {
							
//							if(this.combinerRunner != null) // && values_0.size() >= 4) {
//								this.runCombinerOnBuffer(this.txBuffer);
//							
							this.noOfRecordsOut += txBuffer.size();
							txChannel.Send(txBuffer);
							
							txBuffer = null;
							taskState |= State.BlockedOnTX;
						}
						taskIsRunning = false;

						// indicate progress before we go to sleep
						reporter.progress();
						return;
					}

					// remove the block on rx 1
					taskState &= ~State.BlockedOnRX_1;
				}

				if ((taskState & State.BlockedOnTX) != 0) {

					txBuffer = txChannel.GetEmpty(result);
				  	if (!result.value) {
				  		taskIsRunning = false;
				    		
			    		// indicate progress before we go to sleep
						reporter.progress();
			    		return;
			    	}
			    	// clear the buffer
			    	txBuffer.clear();

			    	// remove the block on tx
				    taskState &= ~State.BlockedOnTX;
				}				
				assert taskState == State.NotBlocked;
			}

			if (rxBuffer_0 == null && rxBuffer_1 == null) {
				// rx channels have completed

				// send remaining buffer (if contains any data)
				if (txBuffer != null && txBuffer.size() > 0) {
					
//					if(this.combinerRunner != null) // && values_0.size() >= 4) {
//						this.runCombinerOnBuffer(this.txBuffer);
					
					this.noOfRecordsOut += txBuffer.size();
					txChannel.Send(txBuffer);
					
					txBuffer = null;
				}

				if(this.noOfRecordsIn != this.noOfRecordsOut)
					System.out.println(this.index + " : in_0-"+ this.zeroValuesRead +" in_1-"+ this.oneValuesRead +
							"\tmissing output="+ (this.noOfRecordsIn - this.noOfRecordsOut) +
							" sent output="+ this.noOfRecordsOut +".");
				
				txChannel.Close();

				// indicate progress before we go to sleep
				reporter.progress();
				return;
			}

			while (taskState == State.NotBlocked) {
				
				if (rxBuffer_0 != null && rxBuffer_1 != null) {
					
					compare = this.comparator.compare(rxBuffer_0.peekKey(), rxBuffer_1.peekKey());

					if (compare < 0) {
					
						this.zeroValuesRead++;
						
						K key = rxBuffer_0.removeKey();
						ArrayList<V> values = rxBuffer_0.removeValues();
						
						txBuffer.AddKeyValues(key, values);
						this.noOfRecordsIn++;
						
						if (rxBuffer_0.size() == 0)
							taskState = State.BlockedOnRX_0;

					} else if (compare > 0) {

						this.oneValuesRead++;

						K key = rxBuffer_1.removeKey();
						ArrayList<V> values = rxBuffer_1.removeValues();

						txBuffer.AddKeyValues(key, values);
						this.noOfRecordsIn++;
						
						if (rxBuffer_1.size() == 0)
							taskState = State.BlockedOnRX_1;
						
					} else { // if(compare == 0)
						
						this.zeroValuesRead++;
						this.oneValuesRead++;
						
						// the keys are the same
						K key_0 = rxBuffer_0.removeKey();
						K key_1 = rxBuffer_1.removeKey();
						assert key_0.equals(key_1);

						// merge the values into one single list
						ArrayList<V> values_0 = rxBuffer_0.removeValues();
						ArrayList<V> values_1 = rxBuffer_1.removeValues();

						values_0.addAll(values_1);

						// TODO here we run the combiner.
						// values_0 now contains both lists of values
//						if(this.combinerRunner != null) {// && values_0.size() >= 4) {
//							this.runCombiner(key_0, values_0, this.txBuffer);
//						} else {
							txBuffer.AddKeyValues(key_0, values_0);
							this.noOfRecordsIn++;
							
//						}

						if (rxBuffer_0.size() == 0)
							taskState = State.BlockedOnRX_0;

						if (rxBuffer_1.size() == 0)
							taskState |= State.BlockedOnRX_1;
					}
				} else if (rxBuffer_0 != null) {

					this.zeroValuesRead++;
					
					K key = rxBuffer_0.removeKey();
					ArrayList<V> values = rxBuffer_0.removeValues();

					txBuffer.AddKeyValues(key, values);
					this.noOfRecordsIn++;
					
					if (rxBuffer_0.size() == 0)
						taskState = State.BlockedOnRX_0;
					
				} else { // if (rxBuffer_1 != null)
					
					this.oneValuesRead++;
					
					K key = rxBuffer_1.removeKey();
					ArrayList<V> values = rxBuffer_1.removeValues();

					txBuffer.AddKeyValues(key, values);
					this.noOfRecordsIn++;
					
					if (rxBuffer_1.size() == 0)
						taskState = State.BlockedOnRX_1;
				}

				if (!txBuffer.hasRemaining()) {
					// TX buffer reached maximum capacity
					taskState |= State.BlockedOnTX;
				}
				reporter.progress(); // indicate we are making progress
			}

			// if the buffers have been consumed, release them
			if ((taskState & State.BlockedOnRX_0) != 0) {
				rxChannel_0.Release(rxBuffer_0);
				rxBuffer_0 = null;
			}

			if ((taskState & State.BlockedOnRX_1) != 0) {
				rxChannel_1.Release(rxBuffer_1);
				rxBuffer_1 = null;
			}

			if ((taskState & State.BlockedOnTX) != 0) {
				
//				if(this.combinerRunner != null) // && values_0.size() >= 4) {
//					this.runCombinerOnBuffer(this.txBuffer);
				
				this.noOfRecordsOut += txBuffer.size();
				txChannel.Send(txBuffer);
				txBuffer = null;
			}
		}
	}
	
	/**
	 * Runs the combiner on a full buffer and the output is put back in the buffer.
	 * @param buffer the buffer to apply the combiner on.
	 * */
	private final void runCombinerOnBuffer(IOChannelBuffer<K, V> buffer) {
		for(int i = 0; i < buffer.size(); i++) {
			K key = buffer.removeKey();
			ArrayList<V> values = buffer.removeValues();
			
			if(values.size() == 1) {
				buffer.AddKeyValues(key, values);
				continue;
			}
			this.runCombiner(key, values, buffer);
		}
	}
	
	private final void runCombiner(K key, ArrayList<V> values, OutputCollector<K, V> collector) {
		try {
			this.combinerRunner.combine(key, values, collector);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
}
