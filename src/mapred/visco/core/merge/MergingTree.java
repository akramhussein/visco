package visco.core.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ReduceTask.ReduceCopier.MapOutputLocation;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task.CombinerRunner;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import visco.util.ActionDelegate;
import visco.util.FuncDelegate;

public class MergingTree <K extends WritableComparable<K>, V extends Writable> {

//	private static final Log LOG = LogFactory.getLog(Reducer.class);
	  
	/**
	 *  an enumeration for the tree node children
	 */	
	private enum ChildType {
		LEFT_CHILD,
		RIGHT_CHILD
	}

	// mergingTasks array for testing
	public static ArrayList<MergingTask> mergingTasks = new ArrayList<MergingTask>();

	/**
	 *  creates the merging tre structure for the reduce phase of the data
	 * @param jobConf
	 *    		the configuration of the job
	 * @param inputPaths
	 * 			the map output locations that are associated with the leaf nodes of the tree
	 * @param jobTokenSecret
	 * 			needed for opening a secure connection to fetch the map outputs	
	 * @param threadPool
	 * 			a pool of threads for the merging tasks
	 * @param counter
	 * 			counter for the records that are read from the map phase
	 * @param reduceInputKeyCounter
	 * 			counter for the input keys in the reduce function
	 * @param reduceInputValueCounter
	 * 			counter for the input values in the reduce function
	 * @param reduceOutputCounter
	 * 			counter for the final output records 
	 * @param codec
	 * 			compression codec for the intermediate data
	 * @param taskId
	 * 			the ID of the reduce task
	 * @param reporter
	 * 			a reporter for the job
	 * @param onMergeCompleted
	 * 			the action to be executed when the reduce finishes
	 * @throws IOException
	 */

	@SuppressWarnings("unchecked")
	public static void createMergingTree(JobConf jobConf, String finalName, 
			List<MapOutputLocation> inputPaths, SecretKey jobTokenSecret,
			Executor threadPool, Counters.Counter counter, 
			Counter reduceInputKeyCounter, Counter reduceInputValueCounter,
			Counter reduceOutputCounter, CompressionCodec codec, 
			TaskAttemptID taskId, CombinerRunner combiner, Reporter reporter, 
			ActionDelegate onMergeCompleted) throws IOException {	

		// create the NetworkIOChannels and connect them with the input paths
		NetworkIOChannel networkChannels[] = new NetworkIOChannel[inputPaths.size()];
		for (int i = 0; i < networkChannels.length; i++) {
			networkChannels[i] = new NetworkIOChannel(jobConf, inputPaths.get(i), jobTokenSecret, 
					codec, counter, taskId.getTaskID().getId(), reporter); 
		}

		/*
		 * We separate two cases. The one is when there is only one map (and 
		 * the task is not scheduled on the same node) and the other is for a 
		 * task with more than one maps.
		 * */
		if(networkChannels.length == 1) {
			// TODO check it
			IndividualMergingTask task = new IndividualMergingTask(threadPool, reporter, jobConf, finalName,
					reduceInputKeyCounter, reduceInputValueCounter, reduceOutputCounter, taskId, onMergeCompleted);
			task.SetRXChannel(networkChannels[0]);
			MergingTree.unblockRXChannel(task);
		} else {

			// create the channel to the final reduce phase for the root node
			IOChannel txChannel = new FinalOutputChannel(jobConf, reporter, reduceInputKeyCounter, 
					reduceInputValueCounter, reduceOutputCounter, taskId, finalName, onMergeCompleted);//null;
//			if(jobConf.getUseNewReducer()) {
//				txChannel = new NewFinalOutputChannel(jobConf, reporter, reduceInputKeyCounter, 
//						reduceInputValueCounter, reduceOutputCounter, taskId, onMergeCompleted);
//			} else { 
//				txChannel = new OldFinalOutputChannel(jobConf, reporter, finalName, 
//						(org.apache.hadoop.mapred.Counters.Counter) reduceOutputCounter, onMergeCompleted);
//			}
		
			// create the root merge task
			MergingTask rootTask = new MergingTask(jobConf, threadPool, null, reporter);// TODO here it was combiner the null
			rootTask.SetTXChannel(txChannel);
			
			// build the rest of tree recursively
			MergingTree.recursiveBuildTree(networkChannels, 0, networkChannels.length,
					rootTask, jobConf, taskId, combiner, reporter);
			MergingTree.unblockTasks();
		}
		System.out.println("# merge tasks: "+ MergingTask.indexCounter);
	}

	/**
	 * Recursively build a tree of merge tasks.
	 * 
	 * @param leaves
	 *            the leaves of the tree (ie the DiskToIOChannels)
	 * @param start
	 *            the start index of the leaves array
	 * @param end
	 *            the end index of the leaves array
	 * @param parent
	 *            the parent merge task (initially it's the root)
	 *            
	 * BUG : input the same channel if we have one input.
	 * attempt_201209250926_0002_r_000000_0: Merging Task 0 : left input [0]
	 * attempt_201209250926_0002_r_000000_0: Merging Task 0 : right input [0]
	 * attempt_201209250926_0002_r_000000_0: # merge tasks: 1
	 * attempt_201209250926_0002_r_000000_0: Exception in thread "pool-1-thread-1" java.util.NoSuchElementException
	 * */
	@SuppressWarnings("unchecked")
	private static void recursiveBuildTree(NetworkIOChannel[] leaves,
			int start, int end, final MergingTask parent, JobConf conf, 
			TaskAttemptID taskId, CombinerRunner combiner, Reporter reporter) {
		assert end - start >= 2;

		if (end - start == 2) {
			parent.SetRXChannels(leaves[start], leaves[start + 1]);
			MergingTree.tasks_for_both.add(parent);
			return;
		}
		

		int split = ((end - start) / 2) + start;

		IOChannel<IOChannelBuffer> leftChannel, rightChannel;
		if (split - start >= 2) {
			// create the left task and connect to the current task using a memory io channel
			MergingTask leftChild = new MergingTask(conf, parent.threadPool, combiner, reporter);
			
			leftChannel = MergingTree.createMemoryChannel(leftChild, parent,
					ChildType.LEFT_CHILD, conf, taskId, reporter);
			leftChild.SetTXChannel(leftChannel);
			recursiveBuildTree(leaves, start, split, leftChild, conf, taskId, combiner, reporter);
			
		} else {
			// there is only one leaf => connect the current node to it
			leftChannel = leaves[start];
			MergingTree.tasks_left.add(parent);
		}

		// do the same for the right channel
		if (end - split >= 2) {
			// create the right task and connect to the current task using a memory io channel
			MergingTask rightChild = new MergingTask(conf, parent.threadPool, combiner, reporter);
			
			rightChannel = MergingTree.createMemoryChannel(rightChild, parent,
					ChildType.RIGHT_CHILD, conf, taskId, reporter);
			rightChild.SetTXChannel(rightChannel);			
			recursiveBuildTree(leaves, split, end, rightChild, conf, taskId, combiner, reporter);
			
		} else {
			// there is only one leaf => connect the current node to it
			rightChannel = leaves[split];
			MergingTree.tasks_right.add(parent);
		}

		// finally set the rx channels
		parent.SetRXChannels(leftChannel, rightChannel);
	}

	private static List<MergingTask> tasks_for_both = new ArrayList<MergingTask>();
	private static List<MergingTask> tasks_left = new ArrayList<MergingTask>();
	private static List<MergingTask> tasks_right = new ArrayList<MergingTask>();
	
	// TODO THIS IS SLOWER THAN BEFORE AND DOES NOT CORRECT THE ERROR ON THE RESULTS.
	private static void unblockTasks() {
		for(MergingTask task : MergingTree.tasks_for_both) {
			MergingTree.unblockRXChannel(task, ChildType.LEFT_CHILD);
			MergingTree.unblockRXChannel(task, ChildType.RIGHT_CHILD);
		}
		
		for(MergingTask task : MergingTree.tasks_left) {
			MergingTree.unblockRXChannel(task, ChildType.LEFT_CHILD);
		}

		for(MergingTask task : MergingTree.tasks_right) {
			MergingTree.unblockRXChannel(task, ChildType.RIGHT_CHILD);
		}
	}
	
	/**
	 * Create a memory channel.
	 * 
	 * @param child
	 *            the child merge task to which this child will be attached
	 * @param parent
	 *            the parent merge task to which this child will be attached
	 * @param childType
	 *            the type of the child
	 * @return a memory channel for an intermediate node of the tree
	 */
	@SuppressWarnings("unchecked")
	private static MemoryIOChannel createMemoryChannel(final MergingTask child, final MergingTask parent,
			final ChildType childType, final JobConf conf, TaskAttemptID taskId, Reporter reporter) {
		
		return new MemoryIOChannel(conf, taskId, 100,
				new FuncDelegate<IOChannelBuffer>() {    
		
			@Override
			public IOChannelBuffer Func() {
				return new IOChannelBuffer(1000, conf);
			}

		}, new ActionDelegate() {

			@Override
			public void action() {	
				child.UnblockTX();
			}
		}, new ActionDelegate() {
			@Override
			public void action() {
				if (childType.equals(ChildType.LEFT_CHILD))
					parent.UnblockRX_0();
				else
					// RIGHT_CHILD
					parent.UnblockRX_1();
			}
		}, reporter);
	}

	/**
	 * Unblock the RX Channel of a merge task.
	 * 
	 * @param childTask
	 * 				the merge task of which to unblock the channel
	 * @param childType
	 * 				the type of child of the task (i.e. left or right child)
	 */
	private static void unblockRXChannel(final MergingTask childTask, 
			final ChildType childType) {
		if (childType.equals(ChildType.LEFT_CHILD)) 
			childTask.UnblockRX_0();
		else
			// RIGHT_CHILD
			childTask.UnblockRX_1();
	}	
	
	private static void unblockRXChannel(final IndividualMergingTask childTask) {
		childTask.UnblockRX_0();
	}


	@SuppressWarnings("unchecked")
	public static void createMergingTree(JobConf jobConf,
			FileSystem fs, Path[] inputPaths, String finalName,
			RawComparator comparator, Executor threadPool, 
			Counters.Counter counter,  Counter reduceInputKeyCounter,
			Counter reduceInputValueCounter,
			Counter reduceOutputCounter, CompressionCodec codec, 
			TaskAttemptID taskId, CombinerRunner combiner, Reporter reporter, 
			ActionDelegate onMergeCompleted, Log logger) throws IOException {	

		// create the NetworkIOChannels and connect them with the input paths
		DiskToIOChannel diskChannels[] = new DiskToIOChannel[inputPaths.length];

		for (int i=0;i<diskChannels.length;i++) {
			diskChannels[i] = new DiskToIOChannel(jobConf, fs, inputPaths[i], codec, counter, reporter);
		}

		// create the channel to the final reduce phase for the root node
		if(diskChannels.length == 1) {
			IndividualMergingTask task = new IndividualMergingTask(threadPool, reporter, jobConf, finalName,
					reduceInputKeyCounter, reduceInputValueCounter, reduceOutputCounter, taskId, onMergeCompleted);
			task.SetRXChannel(diskChannels[0]);
			MergingTree.unblockRXChannel(task);
		} else {
			IOChannel txChannel = new FinalOutputChannel(jobConf, reporter, reduceInputKeyCounter, 
					reduceInputValueCounter, reduceOutputCounter, taskId, finalName, onMergeCompleted);//null;
//			if(jobConf.getUseNewReducer()) {
//				txChannel = new NewFinalOutputChannel(jobConf, reporter, reduceInputKeyCounter, 
//						reduceInputValueCounter, reduceOutputCounter, taskId, onMergeCompleted);
//			} else { 
//				txChannel = new OldFinalOutputChannel(jobConf, 
//						reporter, finalName, (org.apache.hadoop.mapred.Counters.Counter) reduceOutputCounter, onMergeCompleted);
//			}
			// create the root merge task
			MergingTask rootTask = new MergingTask(jobConf, threadPool, null, reporter);
			rootTask.SetTXChannel(txChannel);

			// build the rest of tree recursively
			MergingTree.recursiveBuildTree(diskChannels, 0, diskChannels.length,
					rootTask, jobConf, taskId, combiner, reporter);

		}

		System.out.println("# merge tasks: "+ MergingTask.indexCounter);
	}

	/**
	 * Recursively build a tree of merge tasks.
	 * 
	 * @param leaves
	 *            the leaves of the tree (ie the DiskToIOChannels)
	 * @param start
	 *            the start index of the leaves array
	 * @param end
	 *            the end index of the leaves array
	 * @param parent
	 *            the parent merge task (initially it's the root)
	 */
	@SuppressWarnings("unchecked")
	private static void recursiveBuildTree(DiskToIOChannel[] leaves,
			int start, int end, final MergingTask parent, JobConf conf, 
			TaskAttemptID taskId, CombinerRunner combiner, Reporter reporter) {
		assert end - start >= 2;

		if (end - start == 2) {
			parent.SetRXChannels(leaves[start], leaves[start + 1]);
			MergingTree.unblockRXChannel(parent, ChildType.LEFT_CHILD);
			MergingTree.unblockRXChannel(parent, ChildType.RIGHT_CHILD);
			
			System.out.println("Merging Task "+ parent.getMergingTaskId() +" : inputs ["+ start +", "+ (start + 1) +"]");
			return;
		}

		int split = ((end - start) / 2) + start;

		IOChannel<IOChannelBuffer> leftChannel, rightChannel;
		if (split - start >= 2) {
			// create the left task and connect to the current task using a
			// memory io channel
			MergingTask leftChild = new MergingTask(conf, parent.threadPool, combiner, reporter);
			leftChannel = MergingTree.createMemoryChannel(leftChild, parent,
					ChildType.LEFT_CHILD, conf, taskId, reporter);
			leftChild.SetTXChannel(leftChannel);

			recursiveBuildTree(leaves, start, split, leftChild, conf, taskId, combiner, reporter);
		} else {
			// there is only one leave => connect the current node to it
			leftChannel = leaves[start];
			
			System.out.println("Merging Task "+ parent.getMergingTaskId() +" : left input ["+ start +"]");
		}

		// do the same for the right channel
		if (end - split >= 2) {
			// create the right task and connect to the current task using a
			// memory io channel
			MergingTask rightChild = new MergingTask(conf, parent.threadPool, combiner, reporter);
			rightChannel = MergingTree.createMemoryChannel(rightChild, parent,
					ChildType.RIGHT_CHILD, conf, taskId, reporter);

			rightChild.SetTXChannel(rightChannel);

			recursiveBuildTree(leaves, split, end, rightChild, conf, taskId, combiner, reporter);
		} else {
			// there is only one leave => connect the current node to it
			rightChannel = leaves[split];
			
			System.out.println("Merging Task "+ parent.getMergingTaskId() +" : right input ["+ split +"]");
		}

		// finally set the rx channels
		parent.SetRXChannels(leftChannel, rightChannel);
		MergingTree.unblockRXChannel(parent, ChildType.LEFT_CHILD);
		MergingTree.unblockRXChannel(parent, ChildType.RIGHT_CHILD);	
	}
}
