package visco.util;

/**
 * Encapsulates a method that has no parameters and returns a value of the type
 * specified by the TResult parameter. It resembles the Func delegate in C#.
 */
public abstract class FuncDelegate<T> {
    /**
     * @return The return value of the method that this delegate encapsulates.
     */
    public abstract T Func();
}
