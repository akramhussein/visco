package visco.core.merge;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import visco.util.ModifiableBoolean;

/**
 * A channel to associate each IFile with a leaf of the merging tree.
 *
 * @param <K>
 * 		the key class of the job
 * @param <V>
 *		the value class of the job
 */	

public class DiskToIOChannel <K extends WritableComparable<K>, V extends Writable> 
implements IOChannel<IOChannelBuffer<K,V>> {

	/**
	 * The configuration of the job.
	 */
	private JobConf jobConf;

	/**
	 * The file system of the job.
	 */
	private final FileSystem fs;

	/**
	 * The input path that is associated with this channel.
	 */
	private final Path inputPath;

	/**
	 * The compression codec of the copied map outputs
	 */
	private final CompressionCodec codec;

	/**
	 * A records counter.
	 */
	private final Counter recordsCounter;

	/**
	 * The IO channel buffer that we pass around.
	 */
	private final IOChannelBuffer<K, V> item;

	/**
	 * A reader for the input path
	 */
	private IFile.Reader<K,V> reader;

	/**
	 * The current key we process
	 */
	private K key;

	/**
	 * The next key to process
	 */
	private K  nextKey;

	/**
	 * The current value
	 */
	private V value;

	/**
	 * A reporter for the job
	 */
	private Reporter reporter;

	/**
	 * A deserializer for the keys
	 */
	private Deserializer<K> keyDeserializer;

	/**
	 * A deserializer for the values
	 */
	private Deserializer<V> valDeserializer;

	/**
	 * A comparator for the keys
	 */
	private RawComparator<K> comparator;

	/**
	 * A buffer for the keys
	 */
	private DataInputBuffer kb = new DataInputBuffer();

	/**
	 * A buffer for the values
	 */
	private DataInputBuffer vb = new DataInputBuffer();	


	static int counter = 0;

	/**
	 * @param jobConf
	 * 			the configuration of the job
	 * @param fs
	 * 			the filesystem of the job
	 * @param inputPath
	 * 			the input path for the channel
	 * @param codec
	 * 			the compression codec of the input
	 * @param counter
	 * 			a counter for the inputs
	 */
	@SuppressWarnings("unchecked")
	public DiskToIOChannel(JobConf jobConf, FileSystem fs, 
			Path inputPath, CompressionCodec codec, 
			Counter counter, Reporter reporter) {
		this.jobConf = jobConf;
		this.item = new IOChannelBuffer<K, V>(1000, this.jobConf);
		this.fs = fs;
		this.inputPath = inputPath;
		this.codec = codec;
		this.recordsCounter = counter;
		this.comparator = jobConf.getOutputKeyComparator();
		this.reporter = reporter;
		SerializationFactory serializationFactory = new SerializationFactory(jobConf);
		this.keyDeserializer = serializationFactory.getDeserializer(
				(Class<K>) jobConf.getMapOutputKeyClass());
		this.valDeserializer = serializationFactory.getDeserializer(
				(Class<V>) jobConf.getMapOutputValueClass());
		try {
			this.keyDeserializer.open(this.kb);
			this.valDeserializer.open(this.vb);
			this.reader = new IFile.Reader<K,V>(jobConf, fs, inputPath, codec, recordsCounter);		
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
	}

	@Override
	public IOChannelBuffer<K, V> GetEmpty(ModifiableBoolean result) {
		throw new UnsupportedOperationException(
				"This method should never be called");
	}


	@Override
	public void Send(IOChannelBuffer<K, V> item) {
		throw new UnsupportedOperationException(
				"This method should never be called");
	}


	@Override
	public IOChannelBuffer<K, V> Receive(ModifiableBoolean result) {

		try {			
			while (item.hasRemaining() && reader.next(kb, vb)) {
				key = null;
				value = null;
				key = keyDeserializer.deserialize(key);
				value = valDeserializer.deserialize(value);				

				ArrayList<V> values = new ArrayList<V>();
				values.add(value);
				item.AddKeyValues(key, values);

				reporter.progress(); // notify we are making progress
			}
		} catch (EOFException eof) {
			System.out.println("End of file size: "+item.size());

			try {
				// close the IFile.Reader instance
				reader.close();
			} catch (IOException ioe) {
				ioe.printStackTrace(System.out);
			}

			result.value = true;
			if(item.size() > 0)
				return item;
			return null;
		} catch (IOException ioe) {
			ioe.printStackTrace(System.out);
			return null;
		}

		result.value = true;
		return item;
	}


	@Override
	public void Release(IOChannelBuffer<K, V> item) {
		this.item.clear();
	}


	@Override
	public void Close() {
		throw new UnsupportedOperationException(
				"This method should never be called");
	}
}
