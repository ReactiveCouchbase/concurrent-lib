package org.reactivecouchbase.concurrent.test;

import org.junit.Assert;
import org.junit.Test;
import org.reactivecouchbase.common.Duration;
import org.reactivecouchbase.concurrent.Await;
import org.reactivecouchbase.concurrent.Future;
import org.reactivecouchbase.concurrent.NamedExecutors;
import org.reactivecouchbase.concurrent.Promise;
import org.reactivecouchbase.functional.Action;
import org.reactivecouchbase.functional.Try;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class ConcurrentTest {

    public static ExecutorService ec = NamedExecutors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1, "ConcurrentTestEC");
    public static ScheduledExecutorService sched = NamedExecutors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1, "ConcurrentTestScheduledEC");

    @Test
    public void testAwait() {
        NamedExecutors.newCachedThreadPool("Hello");
        NamedExecutors.newFixedThreadPool(2, "Hello");
        NamedExecutors.newSingleThreadPool("Hello");
        Future<String> fu = Future.successful("Hello");
        Await.result(fu, Duration.parse("2 s"));
        Await.result(fu, 2L, TimeUnit.SECONDS);
        Await.resultForever(fu);
        Await.resultForeverOr(fu, "Goodbye");
        Await.resultOr(fu, "Goodbye", Duration.parse("2 s"));
        Await.resultOr(fu, "Goodbye", 2L, TimeUnit.SECONDS);
    }

    @Test
    public void testFuture() throws Exception {
        final CountDownLatch latch = new CountDownLatch(9);
        Future<Void> future = Future.async((Action<Void>) aVoid -> {
            System.out.println("Async 1");
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            latch.countDown();
        }, ec).map((Action<Void>) aVoid -> {
            System.out.println("Map 1");
            latch.countDown();
        }, ec).map((Action<Void>) aVoid -> {
            System.out.println("Map 2");
            latch.countDown();
        }, ec).filter(aVoid -> {
            System.out.println("Filter");
            latch.countDown();
            return true;
        }, ec).filterNot(aVoid -> {
            System.out.println("FilterNot");
            latch.countDown();
            return false;
        }, ec).flatMap(aVoid -> {
            System.out.println("Flatmap");
            latch.countDown();
            return Future.async((Action<Void>) aVoid1 -> {
                System.out.println("Async 2");
                latch.countDown();
            }, ec);
        }, ec).andThen(voidTry -> {
            System.out.println("andThen");
            latch.countDown();
        }, ec);
        future.foreach(o -> {
            System.out.println("Foreach");
            latch.countDown();
            return null;
        }, ec);
        latch.await(2, TimeUnit.SECONDS);
        Assert.assertEquals(0, latch.getCount());
    }

    @Test
    public void testFutureError() throws Exception {
        final CountDownLatch latch = new CountDownLatch(6);
        final CountDownLatch errorlatch = new CountDownLatch(1);
        Future<Void> future = Future.async((Action<Void>) aVoid -> {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            latch.countDown();
            throw new RuntimeException("Damn it !!!");
        }, ec).map((Function<Void, Void>) aVoid -> {
            errorlatch.countDown();
            return null;
        }, ec).recover((Function<Throwable, Void>) throwable -> {
            latch.countDown();
            return null;
        }, ec);
        future.onError(throwable -> {
            latch.countDown();
        }, ec);
        future.recoverWith(throwable -> {
            latch.countDown();
            return Future.async((Function<Void, Void>) aVoid -> {
                latch.countDown();
                return null;
            }, ec);
        }, ec).map((Function<Void, Void>) aVoid -> {
            latch.countDown();
            return null;
        }, ec);
        latch.await(2, TimeUnit.SECONDS);
        errorlatch.await(2, TimeUnit.SECONDS);
        Assert.assertEquals(0, latch.getCount());
        Assert.assertEquals(1, errorlatch.getCount());
    }

    @Test
    public void testPromise() throws Exception {
        final CountDownLatch latch = new CountDownLatch(8);
        Future<Void> future = Promise.<Void>successful(null).future().map((Action<Void>) aVoid -> {
            System.out.println("Map 1");
            latch.countDown();
        }, ec).map((Action<Void>) aVoid -> {
            System.out.println("Map 2");
            latch.countDown();
        }, ec).filter(aVoid -> {
            System.out.println("Filter");
            latch.countDown();
            return true;
        }, ec).filterNot(aVoid -> {
            System.out.println("FilterNot");
            latch.countDown();
            return false;
        }, ec).flatMap(aVoid -> {
            System.out.println("Flatmap");
            latch.countDown();
            return Future.async((Action<Void>) aVoid1 -> {
                System.out.println("Async 2");
                latch.countDown();
            }, ec);
        }, ec).andThen(voidTry -> {
            System.out.println("andThen");
            latch.countDown();
        }, ec);
        future.foreach(o -> {
            System.out.println("Foreach");
            latch.countDown();
            return null;
        }, ec);
        latch.await(2, TimeUnit.SECONDS);
        Assert.assertEquals(0, latch.getCount());    }

    @Test
    public void testPromiseError() throws Exception {
        final CountDownLatch latch = new CountDownLatch(5);
        final CountDownLatch errorlatch = new CountDownLatch(1);
        Future<Void> future = Promise.<Void>failed(new RuntimeException("Damn it !!!")).future().map(new Function<Void, Void>() {
            @Override
            public Void apply(java.lang.Void aVoid) {
                errorlatch.countDown();
                return null;
            }
        }, ec).recover((Function<Throwable, Void>) throwable -> {
            latch.countDown();
            return null;
        }, ec);
        future.onError(throwable -> {
            latch.countDown();
        }, ec);
        future.recoverWith(throwable -> {
            latch.countDown();
            return Future.async((Function<Void, Void>) aVoid -> {
                latch.countDown();
                return null;
            }, ec);
        }, ec).map((Function<Void, Void>) aVoid -> {
            latch.countDown();
            return null;
        }, ec);
        latch.await(2, TimeUnit.SECONDS);
        errorlatch.await(2, TimeUnit.SECONDS);
        Assert.assertEquals(0, latch.getCount());
        Assert.assertEquals(1, errorlatch.getCount());
    }

    public static <T> List<T> newArrayList(T... items) {
        List<T> list = new ArrayList<>();
        for (T item : items) {
            list.add(item);
        }
        return list;
    }

    @Test
    public void testSequence() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        Future<Void> fu1  = Future.timeout(null, 1L, TimeUnit.SECONDS, sched);
        Future<Void> fu2  = Future.timeout(null, 2L, TimeUnit.SECONDS, sched);
        Future<Void> fu3  = Future.timeout(null, 500L, TimeUnit.MILLISECONDS, sched);
        Future.sequence(newArrayList(fu1, fu2, fu3), sched).onComplete((Action<Try<List<Void>>>) listTry -> latch.countDown(), sched);
        latch.await(5L, TimeUnit.SECONDS);
        Assert.assertEquals(0, latch.getCount());

    }
}
