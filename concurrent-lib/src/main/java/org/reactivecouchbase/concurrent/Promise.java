package org.reactivecouchbase.concurrent;

import org.reactivecouchbase.functional.Failure;
import org.reactivecouchbase.functional.Success;
import org.reactivecouchbase.functional.Try;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class Promise<T> {

    static final ExecutorService INTERNAL_EXECUTION_CONTEXT =
            NamedExecutors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), "FUTURE_INTERNAL_EXECUTION_CONTEXT");

    final CountDownLatch promiseLock;

    AtomicReference<Try<T>> internalResult = new AtomicReference<>(null);

    private final Future<T> future;

    public Promise(ExecutorService ec) {
        this.promiseLock = new CountDownLatch(1);
        this.future = new Future<T>(this, ec);
    }

    public Promise() {
        this.promiseLock = new CountDownLatch(1);
        this.future = new Future<T>(this, INTERNAL_EXECUTION_CONTEXT);
    }

    public boolean isCompleted() {
        return promiseLock.getCount() <= 0;
    }

    public Future<T> future() {
        return future;
    }

    public Promise<T> complete(Try<T> result) {
        synchronized (this) {
            if (!isCompleted()) {
                this.internalResult.set(result);
                promiseLock.countDown();
            } else {
                throw new IllegalStateException("Promise already completed !");
            }
        }
        future.triggerCallbacks();
        return this;
    }

    public Boolean tryComplete(Try<T> result) {
        if (isCompleted()) {
            return false;
        }
        complete(result);
        return true;
    }


    public Promise<T> success(T result) {
        synchronized (this) {
            if (!isCompleted()) {
                this.internalResult.set(new Success<>(result));
                promiseLock.countDown();
            } else {
                throw new IllegalStateException("Promise already completed !");
            }
        }
        future.triggerCallbacks();
        return this;
    }

    public Boolean trySuccess(T result) {
        if (isCompleted()) {
            return false;
        }
        success(result);
        return true;
    }

    public Promise<T> failure(Throwable result) {
        synchronized (this) {
            if (!isCompleted()) {
                this.internalResult.set(new Failure<>(result));
                promiseLock.countDown();
            } else {
                throw new IllegalStateException("Promise already completed !");
            }
        }
        future.triggerCallbacks();
        return this;
    }

    public Boolean tryFailure(Throwable result) {
        if (isCompleted()) {
            return false;
        }
        failure(result);
        return true;
    }

    public static <T> Promise<T> failed(Throwable exception) {
        Promise<T> promise = new Promise<>();
        promise.failure(exception);
        return promise;
    }

    public static <T> Promise<T> successful(T result) {
        Promise<T> promise = new Promise<>();
        promise.success(result);
        return promise;
    }

    public static <T> Promise<T> create() {
        return new Promise<>();
    }

    public static <T> Promise<T> create(ExecutorService ec) {
        return new Promise<>(ec);
    }
}