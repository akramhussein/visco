# Visco

Hadoop with a modified framework to allow for concurrent execution of the Shuffle and Reduce phases.

For more details, visit: http://akramhussein.github.io/visco/

## Objective

To introduce new functionality to the Hadoop codebase (version 1.0.1) that would enable the Map tasks that are completed to be immediately passed to the Reduce stage as opposed to the current method which can only pass 5 tasks at a time. This causes much of the data to be spilled on to disk if the memory is full, a costly performance drawback. Effectively, an overlap of the Shuffle and Reduce stages will occur, therefore delivering a faster runtime and improved network load balancing from the reduced transfer of data across the network.

## Classes

The following classes represent the key componenets of Visco and can be found in src.mapred.visco.*.

`MergingTree` - a binary tree structure that is one of the core classes of our framework. It implements the algorithm for building the tree structure. The main functionality of this class is to associate the different channels with the merging tasks. The merging tasks exist only in the root of each subtree in the tree. Thus, the leaf nodes of the tree contain channels but the intermediate nodes and the root of the tree contain both channels and merging tasks.

`MergingTask` - a thread, which takes data from two input buffers with sorted data and stores the results in a third buffer that is passed to the next level through the Merging-Tree structure. Moreover, it iterates over the data of the two buffers and compares their key values and stores the smaller one in the output buffer, i.e. it sorts and forwards the data in the next level. Note that the input data are already sorted, hence only the smallest one has to be stored in the output buffer in each iteration.

`IOChannel` - base class for all the channel implementations in our framework. It provides functions for allocating memory for a buffer(GetEmpty), deallocating memory for a buffer(Release), add data to the buffer(Send), get data from a buffer(Receive) and a function to denote the end of a stream of processed data (Close). 

`IOChannelBuffer` - buffer implementation that is used to store the data,in a <key,list(values)> representation, in the reduce phase of a job. 

`NonBlockingQueue` - FIFO queue that stores IOChannelBuffer objects, where the current thread is never blocked on any operations. It is used to store data in the intermediate nodes of the tree, the MemoryIOChannels. 

`NetworkIOChannel` - channel implementation that reads data from a network location, deserializes them and stores them in a buffer. Its main components are a URL location, an IOChannelBuffer to store the data and a reader that provides reading functions over the streamed data. 

`MemoryIOChannel` - channel implementation that represents the intermediate nodes of the tree. It contains two NonBlockingQueues of IOChannelBuffers that uses to store the intermediate data. The one queue acts as a buffer pool whereas the other one is the actual storage structure of the channel. 

`FinalOutputChannel` - the channel that the root node of the tree uses to write the final output of a job. As all channels it uses anIOChannelBuffer to store the data but before writing the final output to disk it reduces the data according to the reduce function the user defines in the main program of the job. 

`DiskIOChannel` - deprecated channel implementation that is used to read serialized data from the disk in the initial phase of the MergingTask. It is assigned to the leaf nodes of the Merging-Tree in order to access the input data for the reduce phase. This class was initially created in order to split the work between the team members in order for some members to able to work on the Merging-Tree implementation while the rest could carry out the network data transfer part of the project. As the Merging-Tree now reads directly from the network, this class is no longer used. 
