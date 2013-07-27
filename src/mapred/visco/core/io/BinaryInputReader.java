package visco.core.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.ReduceTask;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.ReduceTask.ReduceCopier.MapOutputLocation;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;

public class BinaryInputReader<K, V> {

	private static final int EOF_MARKER = -1;  

	private static final int DEFAULT_BUFFER_SIZE = 128*1024;
	private static final int MAX_VINT_SIZE = 9;

	/**
	 * The custom http header used for the map output length.
	 */
	public static final String MAP_OUTPUT_LENGTH = "Map-Output-Length";

	/**
	 * The custom http header used for the "raw" map output length.
	 */
	public static final String RAW_MAP_OUTPUT_LENGTH = "Raw-Map-Output-Length";

	/**
	 * The map task from which the map output data is being transferred
	 */
	public static final String FROM_MAP_TASK = "from-map-task";

	/**
	 * The reduce task number for which this map output is being transferred
	 */
	public static final String FOR_REDUCE_TASK = "for-reduce-task";


	// basic/unit connection timeout (in milliseconds)
	private final static int UNIT_CONNECT_TIMEOUT = 30 * 1000;

	/** Number of ms before timing out a copy */
	private static final int STALLED_COPY_TIMEOUT = 3 * 60 * 1000;

	// default read timeout (in milliseconds)
	private final static int DEFAULT_READ_TIMEOUT = 3 * 60 * 1000;
	
	private static final Log LOG = LogFactory.getLog(ReduceTask.class.getName());


	// Count records read from disk
	private long numRecordsRead = 0;
	private final Counters.Counter readRecordsCounter;

	final InputStream in;        // Possibly decompressed stream that we read
	
	Decompressor decompressor;
	
	long bytesRead = 0;
	
	final long fileLength;
	
	boolean eof = false;
	
	final IFileInputStream checksumIn;

	Configuration conf = null;

	byte[] buffer = null;
	
	int bufferSize = DEFAULT_BUFFER_SIZE;
	
	DataInputBuffer dataIn = new DataInputBuffer(); // a buffer to receive data.

	MapOutputLocation loc = null;

	SecretKey jobTokenSecret = null;
	
	URLConnection connection;
	
	InputStream input;
	
	long length;

	private int shuffleConnectionTimeout;
	
	private int shuffleReadTimeout;

	int recNo = 1;

	private int reduce;
	
	/**
	 * Constructs a BinaryInputReader
	 * 
	 * @param conf
	 * 			the configuration of the job
	 * @param codec
	 * @param readsCounter
	 * @param loc
	 * @param jobTokenSecret
	 * @param reduce
	 * @throws IOException
	 */
	public BinaryInputReader(Configuration conf, CompressionCodec codec, Counters.Counter readsCounter, 
			MapOutputLocation loc, SecretKey jobTokenSecret, int reduce) throws IOException {
		
		readRecordsCounter = readsCounter;
		
		//get a jobTokenSecret key for establishing a safe URL connection
		this.jobTokenSecret = jobTokenSecret;
		this.loc = loc;
		this.conf = conf;
		if (conf != null) {
			bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
		}
	
		this.reduce = reduce;
		
		//get InputStream for reading data
		this.input = getURLInputStream(loc);
		this.length = getMapOutputLength();
		if(this.length == -1) {
			throw new IOException("Failed to fetch map-output for "+ 
					loc.getTaskAttemptId() +" from "+ loc.getHost());
		}
	
		this.fileLength = this.length;
		checksumIn = new IFileInputStream(this.input,this.length);
		
		//check if we use compression or not
		if (codec != null) {
			decompressor = CodecPool.getDecompressor(codec);
			this.in = codec.createInputStream(checksumIn, decompressor);
		} else {
			this.in = this.input;
		}
	}

	/**
	 * Returns an InputStream. What happens if the stream is on local disk ?
	 * 
	 * @param location
	 * @return
	 * @throws IOException
	 */
	private InputStream getURLInputStream(MapOutputLocation location) throws IOException {
		// Connect
		URL url = location.getOutputLocation();
		connection = url.openConnection();
		return setupSecureConnection(location, connection);
	}

	/**
	 * sets up a secured connection
	 * 
	 * @param mapOutputLoc
	 * @param connection
	 * @return
	 * @throws IOException
	 */
	private InputStream setupSecureConnection(MapOutputLocation mapOutputLoc, URLConnection connection) throws IOException {

		// generate hash of the url
		String msgToEncode = SecureShuffleUtils.buildMsgFrom(connection.getURL());
		String encHash = SecureShuffleUtils.hashFromString(msgToEncode, this.jobTokenSecret);

		// put url hash into http header
		connection.setRequestProperty(SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);

		shuffleConnectionTimeout = conf.getInt("mapreduce.reduce.shuffle.connect.timeout", STALLED_COPY_TIMEOUT);
		shuffleReadTimeout = conf.getInt("mapreduce.reduce.shuffle.read.timeout", DEFAULT_READ_TIMEOUT);

		InputStream input = getInputStream(connection, shuffleConnectionTimeout, shuffleReadTimeout);

		// get the replyHash which is HMac of the encHash we sent to the server
		String replyHash = connection.getHeaderField(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH);
		if (replyHash == null) {
			throw new IOException("security validation of TT Map output failed");
		}

		// verify that replyHash is HMac of encHash
		SecureShuffleUtils.verifyReply(replyHash, encHash, jobTokenSecret);
		return input;
	}


	/**
	 * The connection establishment is attempted multiple times and is given up only on the last failure. 
	 * Instead of connecting with a timeout of X, we try connecting with a timeout of x < X but multiple times.
	 */
	private InputStream getInputStream(URLConnection connection, int connectionTimeout, int readTimeout) throws IOException {
		int unit = 0;
		if (connectionTimeout < 0) {
			throw new IOException("Invalid timeout [timeout = "+ connectionTimeout +" ms]");
		} else if (connectionTimeout > 0) {
			unit = (UNIT_CONNECT_TIMEOUT > connectionTimeout) ? connectionTimeout : UNIT_CONNECT_TIMEOUT;
		}
		
		// set the read timeout to the total timeout
		connection.setReadTimeout(readTimeout);
		// set the connect timeout to the unit-connect-timeout
		connection.setConnectTimeout(unit);
		while (true) {
			try {
				connection.connect();
				break;
			} catch (IOException ioe) {
				// update the total remaining connect-timeout
				connectionTimeout -= unit;

				// throw an exception if we have waited for timeout
				// amount of time 
				// note that the updated value if timeout is used here
				if (connectionTimeout == 0) {
					throw ioe;
				}

				// reset the connect timeout for the last try
				if (connectionTimeout < unit) {
					unit = connectionTimeout;
					// reset the connect time out for the final connect
					connection.setConnectTimeout(unit);
				}
			}
		}
		try {
			return connection.getInputStream();
		} catch (IOException ioe) {
			throw ioe;
		}
	}
	
	
	/**
	 * @return the length of the entire Map Output
	 */
	private long getMapOutputLength() {

		// Validate header from map output
		TaskAttemptID mapId = null;
		try {
			mapId = TaskAttemptID.forName(connection.getHeaderField(FROM_MAP_TASK));
		} catch (IllegalArgumentException ia) {
			LOG.warn("Invalid map id ", ia);
			return -1;
		}
		

		//check if we are getting data from the expected map task
		TaskAttemptID expectedMapId = loc.getTaskAttemptId();
		if (!mapId.equals(expectedMapId)) {
			LOG.warn("data from wrong map: "+ mapId +", expected map output should be from " + expectedMapId);
			return -1;
		}

		//get decompressedLength from Header of URL connection
		long decompressedLength = Long.parseLong(connection.getHeaderField(RAW_MAP_OUTPUT_LENGTH));  
		//get compressedLength from Header of URL connection
		long compressedLength = Long.parseLong(connection.getHeaderField(MAP_OUTPUT_LENGTH));

		if (compressedLength < 0 || decompressedLength < 0) {
			LOG.warn(" invalid lengths in map output header: id: " +
					mapId + " compressed len: " + compressedLength +
					", decompressed len: " + decompressedLength);
			return -1;
		}
		
		//check if we are getting data for this reduce task  
		int forReduce = (int)Integer.parseInt(connection.getHeaderField(FOR_REDUCE_TASK));
		if (forReduce != reduce) {
			LOG.warn("data for the wrong reduce: " + forReduce +" with compressed len: " + compressedLength );
			return -1;
		}
		return decompressedLength;
	}

	/**
	 * @return the difference between the decompressed and compressed length.
	 * */
	public long getLength() { 
		return fileLength - checksumIn.getSize();
	}

	/**
	 * @return current position
	 * @throws IOException
	 */
	public long getPosition() throws IOException {    
		return checksumIn.getPosition(); 
	}
	
	/**
	 * Read up to len bytes into buf starting at offset off.
	 * 
	 * @param buf buffer 
	 * @param off offset
	 * @param len length of buffer
	 * @return the no. of bytes read
	 * @throws IOException
	 * */
	private int readData(byte[] buf, int off, int len) throws IOException {
		int bytesRead = 0;
		while (bytesRead < len) {
			int n = in.read(buf, off+bytesRead, len-bytesRead);
			
			// if the end of the stream is reached.
			if (n < 0)
				return bytesRead;
			bytesRead += n;
		}
		return len;
	}

	void readNextBlock(int minSize) throws IOException {
		// creates a new buffer
		if (buffer == null) {
			buffer = new byte[bufferSize];
			dataIn.reset(buffer, 0, 0);
		}
		buffer = rejigData(buffer, (bufferSize < minSize) ? new byte[minSize << 1] : buffer);
		bufferSize = buffer.length;
	}

	/**
	 * The way it is used up to now, what it does is write the remaining bytes at the beginning of the buffer and
	 * fill the remaining positions with new data. It is like bytebuffer.compact().
	 * */
	private byte[] rejigData(byte[] source, byte[] destination) throws IOException{
		// Copy remaining data into the destination array
		int bytesRemaining = dataIn.getLength() - dataIn.getPosition();
		if (bytesRemaining > 0) {
			System.arraycopy(source, dataIn.getPosition(), destination, 0, bytesRemaining);
		}

		// Read as much data as will fit from the underlying stream 
		int n = readData(destination, bytesRemaining, (destination.length - bytesRemaining));
		
		dataIn.reset(destination, 0, (bytesRemaining + n));
		return destination;
	}

	/**
	 * Reads the next key,value pair from the received data from the network.
	 * @param key the buffer to receive the key
	 * @param value the buffer to receive the value
	 * @return <code>true</code> if EOF is reached, <code>false</code> otherwise.
	 * */
	public boolean next(DataInputBuffer key, DataInputBuffer value) throws IOException {
		// Sanity check
		if (eof) {
			throw new EOFException("Completed reading " + bytesRead);
		}
		
		// TODO KOSTAS : here we assume that there is only one value associated with each key. Is this correct? 

		// Check if we have enough data to read lengths. 
		
		// TODO yes but if we have not, we just keep reading the lengths.
		// either remove the check or add an else.
		if ((dataIn.getLength() - dataIn.getPosition()) < 2*MAX_VINT_SIZE) {
			readNextBlock(2*MAX_VINT_SIZE);
		}

		// Read key and value lengths
		int oldPos = dataIn.getPosition();
		int keyLength = WritableUtils.readVInt(dataIn);
		int valueLength = WritableUtils.readVInt(dataIn);
		int pos = dataIn.getPosition();
		bytesRead += pos - oldPos;

		// Check for EOF
		if (keyLength == EOF_MARKER && valueLength == EOF_MARKER) {
			eof = true;
			return eof;
		}

		// Sanity check
		if (keyLength < 0)
			throw new IOException("Rec# " + recNo + ": Negative key-length: " + keyLength);
		if (valueLength < 0)
			throw new IOException("Rec# " + recNo + ": Negative value-length: " + valueLength);

		final int recordLength = keyLength + valueLength;

		// Check if we have the raw key/value in the buffer
		if ((dataIn.getLength() - pos) < recordLength) {
			readNextBlock(recordLength);

			// Sanity check
			if ((dataIn.getLength() - dataIn.getPosition()) < recordLength) {
				throw new EOFException("Rec# "+ recNo +" : Could not read the next record");
			}
		}

		// Setup the key and value
		pos = dataIn.getPosition();
		byte[] data = dataIn.getData();
		key.reset(data, pos, keyLength);
		value.reset(data, (pos + keyLength), valueLength);

		// Position for the next record
		long skipped = dataIn.skip(recordLength);
		if (skipped != recordLength) {
			throw new IOException("Rec# " + recNo + ": Failed to skip past record of length: " + recordLength);
		}

		// Record the bytes read
		bytesRead += recordLength;
		++recNo;
		++numRecordsRead;
		return false;
	}

	public long close() throws IOException {
		// Return the decompressor
		if (decompressor != null) {
			decompressor.reset();
			CodecPool.returnDecompressor(decompressor);
			decompressor = null;
		}

		// Close the underlying stream
		in.close();

		if(LOG.isDebugEnabled())
			LOG.debug(this.reduce + " : Read "+ this.bytesRead 
					+" out of "+ this.length +" bytes, or "+ this.numRecordsRead +" records.");
		
		// Release the buffer
		dataIn = null;
		buffer = null;
		if(readRecordsCounter != null) {
			readRecordsCounter.increment(numRecordsRead);
		}
		return this.bytesRead;
	}
}