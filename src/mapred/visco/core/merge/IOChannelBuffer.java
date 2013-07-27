package visco.core.merge;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

/**
 * This is the list of key and values that we pass around during the stages
 */
public class IOChannelBuffer<K extends WritableComparable<K>, V extends Writable> implements OutputCollector<K, V> {
	/**
	 * The list of keys
	 */
	private final ArrayDeque<K> keys = new ArrayDeque<K>();

	/**
	 * The list of values
	 */
	private final ArrayDeque<ArrayList<V>> values = new ArrayDeque<ArrayList<V>>();

	private final JobConf job;
	
	/**
	 * The capacity of this channel buffer
	 */
	private final int capacity;

	/**
	 * @param capacity
	 */
	public IOChannelBuffer(int capacity, JobConf jobConf) {
		this.capacity = capacity;
		this.job = jobConf;
	}

	/**
	 * Add the specified key and values to the list.
	 * 
	 * @param key
	 * @param values
	 */
	public void AddKeyValues(K key, ArrayList<V> values) {
		this.keys.add(key);
		this.values.add(values);
	}

	/**
	 * Retrieves, but does not remove the first key
	 * 
	 * @return the first key
	 */
	public K peekKey() {
		K key = this.keys.peek();

		//assert key != null;
		return key;
	}

	/**
	 * Retrieves and remove the first key
	 * 
	 * @return the first key
	 */
	public K removeKey() {
		K key = this.keys.remove();
		//assert key != null;
		return key;
	}

	/**
	 * Retrieves and remove the first list of values
	 * 
	 * @return the first key
	 */
	public ArrayList<V> removeValues() {
		ArrayList<V> values = this.values.remove();

		//assert values != null;
		return values;
	}

	/**
	 * Removes all of the elements from this channel buffer
	 */
	public void clear() {
		this.keys.clear();
		this.values.clear();
	}

	/**
	 * Returns the number of (key, values) pairs.
	 * 
	 * @return the number of (key, values) pairs
	 */
	public int size() {
		//assert this.keys.size() == this.values.size();
		return this.keys.size();
	}

	/**
	 * Returns <code>true</code> if the channel buffer has still some spare
	 * capacity, <code>false</code> otherwise.
	 * 
	 * @return <code>true</code> if the channel buffer has still some spare
	 *         capacity, <code>false</code> otherwise
	 */
	public boolean hasRemaining() {
		return this.keys.size() < this.capacity;
	}

	public int remaining() {
		return this.capacity - this.keys.size();
	}
	
	@Override
	public void collect(K key, V value) throws IOException {
	
		V copy = WritableUtils.clone(value, this.job);
		
		// there is one pair per key so we do not check if it already exists.
		ArrayList<V> values = new ArrayList<V>();
		values.add(copy);
		
		this.keys.add(key);
		this.values.add(values);
	}
}
