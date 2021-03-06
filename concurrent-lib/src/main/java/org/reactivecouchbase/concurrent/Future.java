package org.reactivecouchbase.concurrent;

import org.reactivecouchbase.common.Duration;
import org.reactivecouchbase.common.Throwables;
import org.reactivecouchbase.functional.*;
import rx.Observable;
import rx.Single;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class Future<T> {

    List<Consumer<Try<T>>> callbacks = new ArrayList<>();
    final ExecutorService ec;
    final Promise<T> promise;
    final java.util.concurrent.Future<T> future;

    Future(final Promise<T> promise, ExecutorService ec) {
        this.ec = ec;
        this.promise = promise;
        this.future = new java.util.concurrent.Future<T>() {
            @Override
            public boolean cancel(boolean b) {
                throw new IllegalAccessError("You can't stop the future !!!");
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return promise.isCompleted();
            }

            @Override
            public T get() throws InterruptedException, ExecutionException {
                promise.promiseLock.await();
                return promise.internalResult.get().get();
            }

            @Override
            public T get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
                promise.promiseLock.await(l, timeUnit);
                if (!promise.isCompleted()) {
                    throw new RuntimeException("Underlying promise is not completed yet.");
                }
                Try<T> tr = promise.internalResult.get();
                if (tr == null) {
                    return null; // should not be null
                }
                return tr.get();
            }
        };
    }

    public Option<Try<T>> getValue() {
        return Option.some(promise.internalResult.get());
    }

    public boolean isSuccess() {
        return promise.internalResult.get().isSuccess();
    }

    public boolean isFailure() {
        return promise.internalResult.get().isFailure();
    }

    private boolean isDone() {
        return promise.isCompleted();
    }

    public java.util.concurrent.Future<T> toJdkFuture() {
        return future;
    }

    public Observable<T> toObservable() { return toObservable(ec); }

    public Observable<T> toObservable(ExecutorService ec) {
        return Observable.create(subscriber -> {
            this.andThen(ttry -> {
                for(T success : ttry.asSuccess()) {
                    subscriber.onNext(success);
                    subscriber.onCompleted();
                }
                for (Throwable t : ttry.asFailure()) {
                    subscriber.onError(t);
                }
            }, ec);
        });
    }

    public Single<T> toSingle() { return toSingle(ec); }

    public Single<T> toSingle(ExecutorService ec) {
        return Single.create(subscriber -> {
            this.andThen(ttry -> {
                for(T success : ttry.asSuccess()) {
                    subscriber.onSuccess(success);
                }
                for (Throwable t : ttry.asFailure()) {
                    subscriber.onError(t);
                }
            }, ec);
        });
    }

    public java.util.concurrent.CompletableFuture<T> toJdkCompletableFuture() {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();
        this.onComplete(tTry -> {
            if (tTry.isSuccess()) {
                completableFuture.complete(tTry.get());
            } else {
                completableFuture.completeExceptionally(tTry.asFailure().get());
            }
        });
        return completableFuture;
    }

    public java.util.concurrent.CompletableFuture<T> toJdkCompletableFuture(ExecutorService ec) {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();
        this.onComplete(tTry -> {
            if (tTry.isSuccess()) {
                completableFuture.complete(tTry.get());
            } else {
                completableFuture.completeExceptionally(tTry.asFailure().get());
            }
        }, ec);
        return completableFuture;
    }

    public static <T> Future<T> fromJdkCompletableFuture(java.util.concurrent.CompletableFuture<T> completableFuture) {
        Promise<T> p = new Promise<>();
        completableFuture.whenComplete((value, exception) -> {
           if (value != null) {
               p.trySuccess(value);
           } else {
               p.tryFailure(exception);
           }
        });
        return p.future();
    }

    public static <T> Future<T> from(java.util.concurrent.CompletableFuture<T> completableFuture) {
        return Future.fromJdkCompletableFuture(completableFuture);
    }

    public static <T> Future<T> from(java.util.concurrent.CompletionStage<T> completableFuture) {
        return Future.fromJdkCompletableFuture(completableFuture.toCompletableFuture());
    }

    void triggerCallbacks() {
        for (final Consumer<Try<T>> block : callbacks) {
            ec.submit((Runnable) () -> block.accept(promise.internalResult.get()));
        }
    }

    public Future<T> andThen(final Consumer<Try<T>> callback, ExecutorService ec) {
        return andThen(Functions.fromConsumer(callback), ec);
    }

    private Future<T> andThen(final Function<Try<T>, Unit> callback, ExecutorService ec) {
        final Promise<T> promise = new Promise<>();
        this.onComplete(r -> {
            callback.apply(r);
            promise.complete(r);
        }, ec);
        return promise.future();
    }

    public void onComplete(final Consumer<Try<T>> callback, ExecutorService ec) {
        synchronized (this) {
            if (!isDone()) {
                callbacks.add(callback);
            }
        }
        if (isDone()) {
            ec.submit((Runnable) () -> callback.accept(promise.internalResult.get()));
        }
    }

    public void onSuccess(final Consumer<T> callback, ExecutorService ec) {
        onSuccess(Functions.fromConsumer(callback), ec);
    }

    public void onSuccess(final Function<T, Unit> callback, ExecutorService ec) {
        onComplete(result -> {
            for (T t : result.asSuccess()) {
                callback.apply(t);
            }
        }, ec);
    }

    public void onError(final Consumer<Throwable> callback, ExecutorService ec) {
        onError(Functions.fromConsumer(callback), ec);
    }

    public void onError(final Function<Throwable, Unit> callback, ExecutorService ec) {
        onComplete(result -> {
            for (Throwable t : result.asFailure()) {
                callback.apply(t);
            }
        }, ec);
    }

    public <B> Future<B> map(final Function<T, B> map, ExecutorService ec) {
        final Promise<B> promise = new Promise<>();
        this.onComplete(result -> {
            for (Throwable t : result.asFailure()) {
                promise.failure(t);
            }
            for (T value : result.asSuccess()) {
                try {
                    promise.trySuccess(map.apply(value));
                } catch (Exception ex) {
                    promise.tryFailure(ex);
                }
            }
        }, ec);
        return promise.future();
    }

    public Future<T> filter(final Function<T, Boolean> predicate, ExecutorService ec) {
        final Promise<T> promise = new Promise<>();
        this.onComplete(result -> {
            for (Throwable t : result.asFailure()) {
                promise.failure(t);
            }
            for (T value : result.asSuccess()) {
                try {
                    if (predicate.apply(value)) {
                        promise.trySuccess(value);
                    }
                } catch (Exception ex) {
                    promise.tryFailure(ex);
                }
            }
        }, ec);
        return promise.future();
    }

    public Future<T> filterNot(final Function<T, Boolean> predicate, ExecutorService ec) {
        final Promise<T> promise = new Promise<>();
        this.onComplete(result -> {
            for (Throwable t : result.asFailure()) {
                promise.failure(t);
            }
            for (T value : result.asSuccess()) {
                try {
                    if (!predicate.apply(value)) {
                        promise.trySuccess(value);
                    }
                } catch (Exception ex) {
                    promise.tryFailure(ex);
                }
            }
        }, ec);
        return promise.future();
    }

    public <B> Future<B> flatMap(final Function<T, Future<B>> map, final ExecutorService ec) {
        final Promise<B> promise = new Promise<>();
        this.onComplete(result -> {
            for (Throwable t : result.asFailure()) {
                promise.failure(t);
            }
            for (T value : result.asSuccess()) {
                try {
                    Future<B> fut = map.apply(value);
                    fut.onComplete(bTry -> {
                        for (Throwable t : bTry.asFailure()) {
                            promise.tryFailure(t);
                        }
                        for (B valueB : bTry.asSuccess()) {
                            promise.trySuccess(valueB);
                        }
                    }, ec);
                } catch (Exception ex) {
                    promise.tryFailure(ex);
                }
            }
        }, ec);
        return promise.future();
    }

    public <S> Future<S> mapTo(final Class<S> clazz, ExecutorService ec) {
        return map(value -> clazz.cast(value), ec);
    }

    public void foreach(final Function<T, ?> block, ExecutorService ec) {
        this.map(t -> block.apply(t), ec);
    }

    public <S> Future<S> transform(final Function<T, S> block, final Function<Throwable, Throwable> errorBlock, ExecutorService ec) {
        final Promise<S> promise = new Promise<>();
        this.onComplete(tTry -> {
            for (final Throwable t : tTry.asFailure()) {
                promise.complete(Try.apply(() -> {
                    throw Throwables.propagate(errorBlock.apply(t));
                }));
            }
            for (final T value : tTry.asSuccess()) {
                promise.complete(Try.apply(() -> block.apply(value)));
            }
        }, ec);
        return promise.future();
    }

    public <U> Future<U> recover(final Function<Throwable, U> block, ExecutorService ec) {
        final Promise<U> promise = new Promise<>();
        this.onComplete(v -> {
            promise.complete(v.recover(block));
        }, ec);
        return promise.future();
    }

    public Future<T> recoverWith(final Function<Throwable, Future<T>> block, final ExecutorService ec) {
        final Promise<T> promise = new Promise<>();
        this.onComplete(v -> {
            for (final Throwable t : v.asFailure()) {
                try {
                    block.apply(t).onComplete(tTry -> {
                        promise.complete(tTry);
                    }, ec);
                } catch (Throwable tr) {
                    promise.failure(tr);
                }
            }
            for (final T value : v.asSuccess()) {
                promise.complete(v);
            }
        }, ec);
        return promise.future();
    }

    public <X> Future<X> fold(Function<Throwable, X> onError, Function<T, X> onSuccess) {
        return fold(onError, onSuccess, this.ec);
    }

    public <X> Future<X> fold(Function<Throwable, X> onError, Function<T, X> onSuccess, final ExecutorService ec) {
        final Promise<X> promise = new Promise<>();
        this.onComplete(v -> {
            for (final Throwable t : v.asFailure()) {
                promise.trySuccess(onError.apply(t));
            }
            for (final T value : v.asSuccess()) {
                promise.trySuccess(onSuccess.apply(value));
            }
        }, ec);
        return promise.future();
    }

    public <X> Future<X> foldM(Function<Throwable, Future<X>> onError, Function<T, Future<X>> onSuccess) {
        return foldM(onError, onSuccess, this.ec);
    }

    public <X> Future<X> foldM(Function<Throwable, Future<X>> onError, Function<T, Future<X>> onSuccess, final ExecutorService ec) {
        final Promise<X> promise = new Promise<>();
        this.onComplete(v -> {
            for (final Throwable t : v.asFailure()) {
                onError.apply(t).andThen(ttry -> {
                   if (ttry.isFailure()) {
                       promise.tryFailure(ttry.asFailure().get());
                   } else {
                       promise.trySuccess(ttry.asSuccess().get());
                   }
                });
            }
            for (final T value : v.asSuccess()) {
                onSuccess.apply(value).andThen(ttry -> {
                    if (ttry.isFailure()) {
                        promise.tryFailure(ttry.asFailure().get());
                    } else {
                        promise.trySuccess(ttry.asSuccess().get());
                    }
                });
            }
        }, ec);
        return promise.future();
    }

    public <X> Future<X> transform(Function<Try<T>, X> trans) {
        return transform(trans, this.ec);
    }

    public <X> Future<X> transform(Function<Try<T>, X> trans, final ExecutorService ec) {
        final Promise<X> promise = new Promise<>();
        this.onComplete(v -> {
            promise.trySuccess(trans.apply(v));
        }, ec);
        return promise.future();
    }

    public <X> Future<X> transformM(Function<Try<T>, Future<X>> trans) {
        return this.transformM(trans, this.ec);
    }

    public <X> Future<X> transformM(Function<Try<T>, Future<X>> trans, final ExecutorService ec) {
        final Promise<X> promise = new Promise<>();
        this.onComplete(v -> {
            trans.apply(v).andThen(ttry -> {
                if (ttry.isFailure()) {
                    promise.tryFailure(ttry.asFailure().get());
                } else {
                    promise.trySuccess(ttry.asSuccess().get());
                }
            });
        }, ec);
        return promise.future();
    }

    public Future<T> fallbackTo(final Future<T> that, final ExecutorService ec) {
        final Promise<T> p = new Promise<>();
        this.onComplete(tTry -> {
            for (Throwable t : tTry.asFailure()) {
                that.onComplete(uTry -> {
                    for (Throwable tr : uTry.asFailure()) {
                        p.complete(tTry);
                    }
                    for (T value : uTry.asSuccess()) {
                        p.complete(uTry);
                    }
                }, ec);
            }
            for (T value : tTry.asSuccess()) {
                p.complete(tTry);
            }
        }, ec);
        return p.future();
    }

    /* Resulting Future will use the  Executor from the current Future */
    public Future<T> andThen(final Consumer<Try<T>> callback) {
        return andThen(callback, ec);
    }

    /* Resulting Future will use the  Executor from the current Future */
    public void onComplete(final Consumer<Try<T>> callback) {
        onComplete(callback, ec);
    }

    /* Resulting Future will use the  Executor from the current Future */
    public void onSuccess(final Consumer<T> callback) {
        onSuccess(Functions.fromConsumer(callback));
    }

    public void onSuccess(final Function<T, Unit> callback) {
        onSuccess(callback, ec);
    }

    /* Resulting Future will use the  Executor from the current Future */
    public void onError(final Consumer<Throwable> callback) {
        onError(Functions.fromConsumer(callback));
    }

    public void onError(final Function<Throwable, Unit> callback) {
        onError(callback, ec);
    }

    /* Resulting Future will use the  Executor from the current Future */
    public <B> Future<B> map(final Function<T, B> map) {
        return map(map, ec);
    }

    /* Resulting Future will use the  Executor from the current Future */
    public Future<T> filter(final Function<T, Boolean> predicate) {
        return filter(predicate, ec);
    }

    /* Resulting Future will use the  Executor from the current Future */
    public Future<T> filterNot(final Function<T, Boolean> predicate) {
        return filterNot(predicate, ec);
    }

    /* Resulting Future will use the  Executor from the current Future */
    public <B> Future<B> flatMap(final Function<T, Future<B>> map) {
        return flatMap(map, ec);
    }

    /* Resulting Future will use the  Executor from the current Future */
    public <S> Future<S> mapTo(final Class<S> clazz) {
        return mapTo(clazz, ec);
    }

    /* Resulting Future will use the  Executor from the current Future */
    public void foreach(final Function<T, ?> block) {
        foreach(block, ec);
    }

    /* Resulting Future will use the  Executor from the current Future */
    public <S> Future<S> transform(final Function<T, S> block, final Function<Throwable, Throwable> errorBlock) {
        return transform(block, errorBlock, ec);
    }

    /* Resulting Future will use the  Executor from the current Future */
    public <U> Future<U> recover(final Function<Throwable, U> block) {
        return recover(block, ec);
    }

    /* Resulting Future will use the  Executor from the current Future */
    public Future<T> recoverWith(final Function<Throwable, Future<T>> block) {
        return recoverWith(block, ec);
    }

    /* Resulting Future will use the  Executor from the current Future */
    public Future<T> fallbackTo(final Future<T> that) {
        return fallbackTo(that, ec);
    }

    public <W> W wrap(Function<Future<T>, W> wrapper) {
        return wrapper.apply(this);
    }

    public static <T> Future<T> firstCompletedOf(final List<Future<T>> futures, final ExecutorService ec) {
        final Promise<T> result = new Promise<>();
        for (Future<T> future : futures) {
            future.onSuccess(t -> {
                result.trySuccess(t);
            }, ec);
        }
        return result.future();
    }

    public static <T> Future<List<T>> sequence(final List<Future<T>> futures, final ExecutorService ec) {
        final Promise<List<T>> result = new Promise<>();
        final List<T> results = Collections.synchronizedList(new ArrayList<>());
        final CountDownLatch latch = new CountDownLatch(futures.size());
        for (Future<T> future : futures) {
            future.onComplete(tTry -> {
                latch.countDown();
                for (Throwable t : tTry.asFailure()) {
                    result.tryFailure(t);
                }
                for (T value : tTry.asSuccess()) {
                    results.add(value);
                }
                if (latch.getCount() == 0) {
                    result.trySuccess(results);
                }
            }, ec);
        }
        if (futures.isEmpty()) {
            result.trySuccess(results);
        }
        return result.future();
    }

    public static Future<Unit> in(Duration duration, final Runnable block, ScheduledExecutorService ec) {
        return in(duration.value, duration.unit, block, ec);
    }

    public static Future<Unit> in(Long in, TimeUnit unit, final Runnable block, ScheduledExecutorService ec) {
        return in(in, unit, () -> {
            block.run();
            return Unit.unit();
        }, ec);
    }

    public static <T> Future<T> in(Duration duration, final Supplier<T> block, ScheduledExecutorService ec) {
        return in(duration.value, duration.unit, block, ec);
    }

    public static <T> Future<T> in(Long in, TimeUnit unit, final Supplier<T> block, ScheduledExecutorService ec) {
        final Promise<T> promise = new Promise<>(ec);
        ec.schedule((Runnable) () -> {
            try {
                promise.trySuccess(block.get());
            } catch (Throwable e) {
                promise.tryFailure(e);
            }
        }, in, unit);
        return promise.future();
    }

    public static <T> Future<T> async(final Supplier<T> block, ExecutorService ec) {
        final Promise<T> promise = new Promise<>(ec);
        ec.submit((Runnable) () -> {
            try {
                promise.trySuccess(block.get());
            } catch (Throwable e) {
                promise.tryFailure(e);
            }
        });
        return promise.future();
    }

    public static Future<Unit> async(final Runnable block, ExecutorService ec) {
        final Promise<Unit> promise = new Promise<>(ec);
        ec.submit((Runnable) () -> {
            try {
                block.run();
                promise.trySuccess(Unit.unit());
            } catch (Throwable e) {
                promise.tryFailure(e);
            }
        });
        return promise.future();
    }

    public static <T> Future<T> failed(Throwable exception) {
        return new Promise<T>().failure(exception).future();
    }

    public static <T> Future<T> successful(T result) {
        return new Promise<T>().success(result).future();
    }

    public static <T> Future<T> fromJdkFuture(final java.util.concurrent.Future<T> future, final ScheduledExecutorService ec) {
        final Promise<T> promise = new Promise<>();
        Runnable wait = new Runnable() {
            @Override
            public void run() {
                if (future.isCancelled()) {
                    promise.tryFailure(new CancellationException("Future has been cancelled"));
                } else if (future.isDone()) {
                    try {
                        promise.trySuccess(future.get());
                    } catch (Exception e) {
                        promise.tryFailure(e);
                    }
                } else {
                    ec.schedule(this, 100, TimeUnit.MILLISECONDS);
                }
            }
        };
        ec.schedule(wait, 10, TimeUnit.MILLISECONDS);
        return promise.future();
    }

    public static <T> Future<T> timeout(final T some, Duration duration, ScheduledExecutorService ec) {
        return timeout(some, duration.value, duration.unit, ec);
    }

    public static <T> Future<T> timeout(final T some, Long in, TimeUnit unit, ScheduledExecutorService ec) {
        return in(in, unit, () -> some, ec);
    }

    private static <T> void retryPromise(final int times, final int wait, final boolean expo, final Promise<T> promise, final Option<Throwable> failure, final Supplier<Future<T>> f, final ScheduledExecutorService ec) {

        if (times == 0) {
            if (failure.isDefined()) {
                promise.tryFailure(failure.get());
            } else {
                promise.tryFailure(new RuntimeException("Failure, but lost track of exception :-("));
            }
        } else {
            Future<T> future = f.get();
            if (future == null) {
                promise.tryFailure(new NullPointerException("Function is return a null future"));
            } else {
                future.onComplete(tTry -> {
                    if (tTry.isSuccess()) {
                        promise.trySuccess(tTry.get());
                    } else {
                        // System.out.println("waiting " + wait + " milliseconds");
                        Future.in(new Duration(wait, TimeUnit.MILLISECONDS), () -> {
                            int newWait = wait;
                            if (wait == 0 && expo) {
                                newWait = 1;
                            } else {
                                if (expo) newWait = newWait + wait;
                            }
                            retryPromise(times - 1, newWait, expo, promise, tTry.error(), f, ec);
                            return Unit.unit();
                        }, ec);
                    }
                }, ec);
            }
        }
    }

    public static <T> Future<T> retry(int times, boolean exponential, Supplier<Future<T>> f, ScheduledExecutorService ec) {
        Promise<T> promise = new Promise<T>();
        retryPromise(times, 0, exponential, promise, Option.<Throwable>none(), f, ec);
        return promise.future();
    }

    public static <T> Future<T> retryExponential(int times, Supplier<Future<T>> f, ScheduledExecutorService ec) {
        return retry(times, true, f, ec);
    }

    public static <T> Future<T> retry(int times, Supplier<Future<T>> f, ScheduledExecutorService ec) {
        return retry(times, false, f, ec);
    }
}
