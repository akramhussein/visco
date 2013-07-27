package visco.core.merge;

import visco.util.ModifiableBoolean;

/**
 * This interface represent an IO channel to communicate between different task
 * stages.
 */
public interface IOChannel<T> {
    /**
     * Producer API: GetEmpty() to receive an empty T to write into.
     * <code>result</code> is <code>true</code> if we got a T, or
     * <code>false</code> if we would block. The producerUnblocked callback will
     * be called in this case.
     */
    T GetEmpty(ModifiableBoolean result);

    /** followed by a Send() of the T: */
    void Send(T item);

    /**
     * Consumer API: Receive() to receive a full T to read. The
     * <code>result</code> is <code>true</code> if we get a T, or
     * <code>false</code> if we should block. The consumerUnblocked callback will
     * be called in this case. If the channel is closed, Receive() returns
     * default(T)
     */
    T Receive(ModifiableBoolean result);

    /**
     * followed by a Release() of the T back to the producer:
     * 
     * @param item
     */
    void Release(T item);

    /**
     * Used by the producer to signal the end of stream. It is a non-blocking
     * operation.
     */
    void Close();

}