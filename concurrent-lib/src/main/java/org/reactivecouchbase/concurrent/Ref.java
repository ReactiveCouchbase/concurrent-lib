package org.reactivecouchbase.concurrent;

import org.reactivecouchbase.common.IdGenerators;
import org.reactivecouchbase.functional.Option;
import org.reactivecouchbase.functional.Unit;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

public class Ref<T> {

    public static <T> Ref<T> empty() {
        return new Ref<T>(IdGenerators.generateUUID());
    }

    public static <T> Ref<T> of(T value) {
        return new Ref<T>(IdGenerators.generateUUID(), Option.some(value));
    }

    public static <T> Ref<T> empty(String name) {
        return new Ref<>(name);
    }

    public static <T> Ref<T> of(T value, String name) {
        return new Ref<>(name, Option.some(value));
    }

    private final AtomicReference<Option<T>> ref = new AtomicReference<>(Option.<T>none());
    private final String name;

    private Ref(String name, Option<T> opt) {
        this.ref.set(opt);
        this.name = name;
    }

    private Ref(String name) {
        this.name = name;
        this.ref.set(Option.<T>none());
    }

    public Ref<T> set(T value) {
        ref.set(Option.apply(value));
        return this;
    }

    public Option<T> getAndSet(T value) {
        return ref.getAndSet(Option.apply(value));
    }

    public T get() {
        return ref.get().getOrThrow(new RuntimeException("Reference to " + name + " was not properly initialized ..."));
    }

    public T apply() {
        return get();
    }

    public Option<T> cleanup() {
        return ref.getAndSet(Option.<T>none());
    }

    public Option<T> asOption() {
        return ref.get();
    }

    public Ref<T> setIfEmpty(Function<Unit, T> f) {
        if (isEmpty()) {
            set(f.apply(Unit.unit()));
        }
        return this;
    }

    public Ref<T> combine(Function<T, Unit> f) {
        if (isDefined()) {
            f.apply(get());
        }
        return this;
    }

    public Ref<T> combineOpt(Function<Option<T>, Unit> f) {
        f.apply(ref.get());
        return this;
    }

    public boolean isEmpty() {
        return ref.get().isEmpty();
    }

    public boolean isDefined() {
        return ref.get().isDefined();
    }

    public T getOrElse(T def) {
        return ref.get().getOrElse(def);
    }

    public <B> Ref<B> map(Function<T, B> f) {
        return new Ref<>(name, ref.get().map(f));
    }

    public <B> B fold(Function<Unit, B> ifEmpty, Function<T, B> f) {
        return ref.get().fold(ifEmpty, f);
    }

    public <B> Ref<B> flatMap(final Function<T, Ref<B>> f) {
        return new Ref<B>(name, ref.get().flatMap(t -> {
            if (f == null) {
                return Option.none();
            }
            Ref<B> res = f.apply(t);
            if (res == null) {
                return Option.none();
            }
            return res.asOption();
        }));
    }

    public Ref<T> filter(Predicate<T> f) {
        return new Ref<>(name, ref.get().filter(f));
    }

    public Ref<T> filterNot(Predicate<T> f) {
        return new Ref<>(name, ref.get().filterNot(f));
    }

    public boolean nonEmpty() {
        return !ref.get().isEmpty();
    }

    public boolean exists(Function<T, Boolean> f) {
        return ref.get().exists(f);
    }

    public boolean forall(Function<T, Boolean> f) {
        return ref.get().forall(f);
    }

    public <U> void foreach(Function<T, U> f) {
        ref.get().foreach(f);
    }

    public <U> void call(Function<T, U> f) {
        ref.get().foreach(f);
    }
}
