package visco.core.merge;

import visco.util.ActionDelegate;
import visco.util.ModifiableBoolean;

/**
 * FIFO queue, where the current thread is never blocked on any operations.
 * Instead, blocking operations return false, and guarantee to call the
 * appropriate callback only once. The callback should do minimal work, since it
 * is on a "stolen" thread (actually, the thread which caused the unblock).
 */
public class NonBlockingQueue<T> {
	private Object qLock = new Object();

	private T[] queue; // circular buffer of data items themselves
	private int readIdx; // next index to be read
	private int writeIdx; // next index to be written into
	private int nitems; // count of items in the queue
	private ActionDelegate enqueueMaybePossible;
	private ActionDelegate dequeueMaybePossible;

	/**
	 * Create a new NonBlockingQueue, able to hold up to <code>maxItems</code>
	 * 
	 * @param maxItems
	 *            Size of queue, in items.
	 * @param enqueueMaybePossible
	 *            The Enqueue operation might now // succeed.
	 * @param dequeueMaybePossible
	 *            The Dequeue operation might now // succeed.
	 */
	@SuppressWarnings("unchecked")
	public NonBlockingQueue(int maxItems, ActionDelegate enqueueMaybePossible,
			ActionDelegate dequeueMaybePossible) {
		if (maxItems <= 0)
			throw new IllegalArgumentException("maxItems must be >0");
		if (enqueueMaybePossible == null)
			throw new IllegalArgumentException("enqueueMaybePossible");
		if (dequeueMaybePossible == null)
			throw new IllegalArgumentException("dequeueMaybePossible");

		/*
		 * This is rather ugly but it is due to the unability of java to
		 * instantiate array of generic type. More info here
		 * http://www.ibm.com/developerworks/java/library/j-jtp01255/index.html
		 */
		this.queue = (T[]) new Object[maxItems];
		this.enqueueMaybePossible = enqueueMaybePossible;
		this.dequeueMaybePossible = dequeueMaybePossible;
	}

	/**
	 * Add an item to the queue. Blocks the current thread if the queue is
	 * currently full.
	 * 
	 * @param item
	 *            Item to be added
	 * @return <code>true</code> if T was successfully enqueued,
	 *         <code>false</code> if the operation // would block, in which case
	 *         the enqueueMaybePossible callback will be // invoked at some
	 *         future time.
	 */
	public boolean Enqueue(T item) {
		boolean wake = false;
		synchronized (this.qLock) {
			/* potentially block, waiting for space in the queue */
			if (this.nitems >= this.queue.length)
				return false;

			/* enqueue this item */
			this.queue[this.writeIdx] = item;
			this.writeIdx = (this.writeIdx + 1) % this.queue.length;
			this.nitems++;

			/* 0->1 transition: wake any Dequeue()s which might be waiting */
			if (this.nitems == 1)
				wake = true;
		}
		if (wake)
			this.dequeueMaybePossible.action();
		return true;
	}

	/**
	 * Remove the next item from the queue. It sets the <code>result</code>
	 * parameter to <code>false</code> if the queue is empty. In this case the
	 * dequeueMaybePossible callback will be called as some point in the future.
	 * 
	 * @param result
	 *            This parameter is set by the function to <code>true</code> if
	 *            the item was successfully dequeued, <code>false</code> if the
	 *            queue is empty.
	 * 
	 * @return Data item which was dequeued
	 */
	public T Dequeue(ModifiableBoolean result) {
		boolean wake = false;
		T item;

		synchronized (this.qLock) {
			/* potentially block, waiting for an item to be available */
			if (this.nitems == 0) {
				result.value = false;
				return null;
			}

			/* dequeue an item */
			item = this.queue[this.readIdx];
			this.readIdx = (this.readIdx + 1) % this.queue.length;
			this.nitems--;

			/* full -> space free: wake any Enqueue()s which might be waiting */
			if (this.nitems == this.queue.length - 1)
				wake = true;
		}
		if (wake)
			this.enqueueMaybePossible.action();

		result.value = true;
		return item;
	}

	/**
	 * @return Returns a spot measurement of the current queue length, in items.
	 */
	public int Count() {
		synchronized (this.qLock) {
			return this.nitems;
		}
	}
}
