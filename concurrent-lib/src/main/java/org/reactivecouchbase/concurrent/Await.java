package org.reactivecouchbase.concurrent;

import org.reactivecouchbase.common.Duration;
import org.reactivecouchbase.common.Invariant;
import org.reactivecouchbase.common.Throwables;

import java.util.concurrent.TimeUnit;

/**
 * Utility to await future based computations
 */
public class Await {

    private Await() {
    }

    public static <T> T result(Future<T> future, Long timeout, TimeUnit unit) {
        Invariant.checkNotNull(future);
        try {
            return future.toJdkFuture().get(timeout, unit);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T result(Future<T> future, Duration duration) {
        Invariant.checkNotNull(future);
        try {
            return future.toJdkFuture().get(duration.value, duration.unit);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T resultForever(Future<T> future) {
        Invariant.checkNotNull(future);
        try {
            return future.toJdkFuture().get(Long.MAX_VALUE, TimeUnit.HOURS);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T resultOr(Future<T> future, T defaultValue, Long timeout, TimeUnit unit) {
        Invariant.checkNotNull(future);
        try {
            return future.toJdkFuture().get(timeout, unit);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static <T> T resultOr(Future<T> future, T defaultValue, Duration duration) {
        Invariant.checkNotNull(future);
        try {
            return future.toJdkFuture().get(duration.value, duration.unit);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static <T> T resultForeverOr(Future<T> future, T defaultValue) {
        Invariant.checkNotNull(future);
        try {
            return future.toJdkFuture().get(Long.MAX_VALUE, TimeUnit.HOURS);
        } catch (Exception e) {
            return defaultValue;
        }
    }
}
