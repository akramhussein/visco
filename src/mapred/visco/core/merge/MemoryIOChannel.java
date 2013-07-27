package visco.core.merge;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import visco.util.ActionDelegate;
import visco.util.FuncDelegate;
import visco.util.ModifiableBoolean;

/**
 * Manage a circular flow of IOChannelBuffers from a producer to consumer.
 */

public class MemoryIOChannel <K extends WritableComparable<K>, V extends Writable> 
implements IOChannel<IOChannelBuffer<K,V>> {

	/**
	 * internally, we represent this as two non-blocking queues, one for Ts
	 * which are full of data (the tx side), and one for returning empty T's
	 * (the rx side):
	 */
	private NonBlockingQueue<IOChannelBuffer<K,V>> tx;

	/**
	 * internally, we represent this as two non-blocking queues, one for Ts
	 * which are full of data (the tx side), and one for returning empty T's
	 * (the rx side):
	 */
	private NonBlockingQueue<IOChannelBuffer<K,V>> rx;

	/**
	 * is the channel closed?
	 */
	private boolean isClosed = false;

	/**
	 * callback to wake-up the consumer
	 * */
	private ActionDelegate consumerUnblocked;

	/**
	 * Empty delegate
	 */
	private static final ActionDelegate NOP = new ActionDelegate() {

		@Override
		public void action() {
			// We know no-one can be waiting for these wake-up cases,
			// but they get fired anyway. Ignore them.
		}
	};

	/**
	 * Make a new IoChannel, that is maxItems deep. We call teeSource() to
	 * pre-allocate those empty items, and make them all initially available to
	 * the producer. We take two callbacks, which are invoked whenever the
	 * producer or consumer have been unblocked.
	 * 
	 * @param maxItems
	 * @param teeSource
	 * @param producerUnblocked
	 * @param consumerUnblocked
	 */
	public MemoryIOChannel(JobConf jobConf, TaskAttemptID taskId, int maxItems, 
			FuncDelegate<IOChannelBuffer<K,V>> teeSource, ActionDelegate producerUnblocked, 
			ActionDelegate consumerUnblocked, Reporter reporter) {
		this.tx = new NonBlockingQueue<IOChannelBuffer<K,V>>(maxItems, NOP, consumerUnblocked);
		this.rx = new NonBlockingQueue<IOChannelBuffer<K,V>>(maxItems, NOP, producerUnblocked);
		for (int i = 0; i < maxItems; i++)
			this.rx.Enqueue(teeSource.Func());

		this.consumerUnblocked = consumerUnblocked;
	}

	@Override
	public IOChannelBuffer<K,V> GetEmpty(ModifiableBoolean result) {	
		return this.rx.Dequeue(result);
	}

	@Override
	public void Send(IOChannelBuffer<K,V> item) {
		if (!this.tx.Enqueue(item))
			throw new RuntimeException("Send would block: can't happen: duplicate Send?");
	}

	@Override
	public IOChannelBuffer<K, V> Receive(ModifiableBoolean result) {

		// fetch the next element from the queue
		IOChannelBuffer<K, V> item = this.tx.Dequeue(result);

		if (result.value) {
			// we have a non-null element to return
			//assert item != null;
			return item;
		} else if (this.isClosed) {


			if(tx.Count() != 0)
				System.out.println(this.tx.Count());
			
			/*
			 * we don't have any more elements to return but this channel has
			 * been closed. So we should return null but set the result value to
			 * true.
			 */
			result.value = true;
			return null;
		} else {
			/* no elements to return and the channel it is still open */
			//assert item == null;
			return null;
		}
	}

	@Override
	public void Release(IOChannelBuffer<K, V> item) {		
		if (!this.rx.Enqueue(item))
			throw new RuntimeException("Release would block: can't happen: attempt to double-free?");
	}

	@Override
	public void Close() {
		isClosed = true;

		// wake-up the consumer
		consumerUnblocked.action();
	}

//	public boolean getIsClosed() {
//		return isClosed;
//	}

//	// for the purpose of testing, since RX is private
//	public void setRXDequeue(ModifiableBoolean MB) {
//		rx.Dequeue(MB);
//	}
//
//	// for the purpose of testing, since RX is private
//	public void setTXEnqueue(IOChannelBuffer<K, V> item) {
//		tx.Enqueue(item);
//	}

}
