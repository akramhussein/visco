package visco.core.merge;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import javax.crypto.SecretKey;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ReduceTask.ReduceCopier.MapOutputLocation;
import org.apache.hadoop.mapred.Reporter;

import visco.core.io.BinaryInputReader;
import visco.util.ModifiableBoolean;


/**
 * This class implements the IIOChannel interface to read data from the network
 * and make it available to the RootWorkerTask
 * 
 * @param <K>
 */
public class NetworkIOChannel<K extends WritableComparable<K>, V extends Writable> implements IOChannel<IOChannelBuffer<K, V>> {

	/**
	 * The IO channel buffer that we pass around.
	 */
	private final IOChannelBuffer<K, V> item;

	/**
	 * The map output location associated with this network channel
	 */
	private MapOutputLocation inputLocation;

	/**
	 * The job configuration
	 */
	private JobConf jobConf;
		
	/**
	 * A key deserialiser
	 */
	private Deserializer<K> keyDeserializer;
	
	/**
	 * A value deserialiser
	 */
	private Deserializer<V> valueDeserializer;
	
	/**
	 * A buffer to store the serialised keys
	 */
	private DataInputBuffer kb = new DataInputBuffer();
	
	/**
	 * A buffer to store the serialised values
	 */
	private DataInputBuffer vb = new DataInputBuffer();
	
	/**
	 * A reader that reads data over the network
	 */
	private BinaryInputReader<K,V> reader;
	
	/**
	 * A key class
	 */
	private K key;
	
	private final RawComparator<K> comparator;
	
	/**
	 * A value class
	 */
	private V value;
	
	/**
	 * The reporter for the job
	 */
	private Reporter reporter;

	private static final Class<?>[] EMPTY_ARRAY = new Class[]{};

	/**
	 * Construct a NetworkIOChannel
	 * 
	 * @param jobConf
	 * 			the job configuration
	 * @param inputLocation
	 * 			the input network location
	 * @param jobTokenSecret
	 * 			a token secret for a secure http connection
	 * @param codec
	 * 			the codec of the data if encoded
	 * @param counter
	 * 			a counter for the inout pairs
	 * @param reduce
	 * 			the number if reduce node
	 * @param reporter
	 * 			a reporter for the job
	 * @throws IOException
	 */
	public NetworkIOChannel(JobConf jobConf, MapOutputLocation inputLocation,
			SecretKey jobTokenSecret, CompressionCodec codec, 
			Counter counter, int reduce, Reporter reporter) throws IOException {
		
		this.jobConf = jobConf;
		this.inputLocation = inputLocation;
		this.reporter = reporter;
		
		SerializationFactory serializationFactory = new SerializationFactory(jobConf);
		this.keyDeserializer = serializationFactory.getDeserializer((Class<K>) jobConf.getMapOutputKeyClass());
		this.valueDeserializer = serializationFactory.getDeserializer((Class<V>) jobConf.getMapOutputValueClass());
		this.keyDeserializer.open(this.kb);
		this.valueDeserializer.open(this.vb);

		this.item = new IOChannelBuffer<K, V>(100, this.jobConf);
		this.comparator = this.jobConf.getOutputKeyComparator();//.getOutputValueGroupingComparator();
		
		this.reader = new BinaryInputReader<K,V>(jobConf, codec, counter, inputLocation, jobTokenSecret, reduce);
	}
	
	@Override
	public IOChannelBuffer<K, V> GetEmpty(ModifiableBoolean result) {
		throw new UnsupportedOperationException("This method should never be called");
	}

	@Override
	public void Send(IOChannelBuffer<K, V> item) {
		throw new UnsupportedOperationException("This method should never be called");
	}

	private boolean isFinished = false;
	
	
	@Override
	public IOChannelBuffer<K, V> Receive(ModifiableBoolean result) {
		if(isFinished) {
			result.value = true;
			return ((item.size() == 0) ? null : item);
		}
		
		try{
			// have to change the structure here so that the values of the same key all go to the same buffer.
			while (item.hasRemaining() && !(isFinished = reader.next(kb, vb))) {
				key = null;
				value  = null;
								
				key = this.keyDeserializer.deserialize(key);
				value = this.valueDeserializer.deserialize(value);

				ArrayList<V> values = null;
//				if (this.lastKey != null && this.comparator.compare(key, this.lastKey) == 0) {
//					values = item.removeValues();
//					item.removeKey();
//				} else {
					values = new ArrayList<V>();
				//}				
				values.add(value);
				item.AddKeyValues(key, values);	

				this.lastKey = key;
				
				reporter.progress();
			}
			
		} catch (EOFException eof) {
			try {
				// close the IFile.Reader instance
				reader.close();
			} catch (IOException ioe) {
				ioe.printStackTrace(System.out);
			}
			result.value = true;
			return (item.size() >  0) ? item : null;
		} catch (IOException ioe) {
			ioe.printStackTrace(System.out);
			return null;
		}
		
		try{
			if(isFinished) {
				reader.close();
				result.value = true;
				reporter.progress();
				return ((item.size() == 0) ? null : item);
			}
		} catch (IOException ioe) {
			ioe.printStackTrace(System.out);
		}
			
		result.value = true;
		return ((item.size() == 0) ? null : item);
	}

	private K lastKey;
		
	private ArrayList<V> values;
	
//	private int dups;
//	private int totalValues;
	
//	@Override
//	public IOChannelBuffer<K, V> Receive(ModifiableBoolean result) {
//		if(isFinished) {
//			result.value = true;
//			return ((item.size() == 0) ? null : item);
//		}
//		
//		try{
//			
//			while (!(isFinished = reader.next(kb, vb))) {	
//				key = null;
//				value  = null;
//								
//				key = this.keyDeserializer.deserialize(key);
//				value = this.valueDeserializer.deserialize(value);
//				
//				boolean isEqualToPrevious = (this.lastKey == null) ? 
//						false : (this.comparator.compare(key, this.lastKey) == 0);
//				
//				if((item.remaining() == 1) && this.lastKey != null && !isEqualToPrevious) {
//					item.AddKeyValues(lastKey, values);	
//	
//					// be ready to put them in the next call.
//					lastKey = key;
//					values = new ArrayList<V>();
//					values.add(value);
//					break;
//				}
//
//				if (isEqualToPrevious) {
//					values.add(value);
//				} else {
//					if(lastKey != null)
//						item.AddKeyValues(lastKey, values);
//					lastKey = key;
//					values = new ArrayList<V>();
//					values.add(value);
//				}
//				reporter.progress();				
//			} 
//			// TODO fix the key to null for the case that there is one record remaining.
//		} catch (EOFException eof) {
//			try {
//				// close the IFile.Reader instance
//				item.AddKeyValues(lastKey, values);
//				reader.close();
//			} catch (IOException ioe) {
//				ioe.printStackTrace(System.out);
//			}
//			result.value = true;
//			return (item.size() >  0) ? item : null;
//		} catch (IOException ioe) {
//			ioe.printStackTrace(System.out);
//			return null;
//		}
//		
//		try{
//			if(isFinished) {
//				item.AddKeyValues(lastKey, values);
//				reader.close();
//				result.value = true;
//				reporter.progress();
//				return ((item.size() == 0) ? null : item);
//			}
//		} catch (IOException ioe) {
//			ioe.printStackTrace(System.out);
//		}
//		
//		result.value = true;
//		return ((item.size() == 0) ? null : item);
//	}
	
	@Override
	public void Release(IOChannelBuffer<K, V> item) {
		this.item.clear();
	}

	@Override
	public void Close() {
		throw new UnsupportedOperationException("This method should never be called");
	}
}