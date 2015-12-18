/* The MIT License (MIT)
 * Copyright (c) 2015 DataRank, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.datarank.tempest.allocator;

import java.util.*;

/**
 * Provides random selection without replacement and constant-time contains() functionality.
 * @param <E>
 */
public class RandomList<E> implements List<E> {
    private final List<E> stableList;
    private final List<E> randomizedList;
    private final Set<E> set;
    private final Random random;

    public RandomList(final Random random) {
        set = new HashSet<>();
        stableList = new ArrayList<>();
        randomizedList = new ArrayList<>();
        this.random = random;
    }

    public RandomList(final int capacity, final Random random) {
        set = new HashSet<>(capacity);
        stableList = new ArrayList<>(capacity);
        randomizedList = new ArrayList<>(capacity);
        this.random = random;
    }

    public RandomList(final RandomList other) {
        set = new HashSet<>(other.getSet());
        stableList = new ArrayList<>();
        randomizedList = new ArrayList<>(other.getRandomizedList());
        random = other.getRandom();
    }

    public RandomList(final List other, final Random random) {
        set = new HashSet<>(other);
        stableList = new ArrayList<>(other);
        randomizedList = new ArrayList<>(other);
        this.random = random;
    }

    @Override
    public int size() {
        return set.size();
    }

    @Override
    public boolean isEmpty() {
        return set.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return set.contains(o);
    }

    /**
     * Random sampling without replacement
     * @return
     */
    @Override
    public Iterator<E> iterator() {
        Collections.shuffle(randomizedList, random);
        return randomizedList.iterator();
    }

    @Override
    public Object[] toArray() {
        return randomizedList.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return randomizedList.toArray(a);
    }

    @Override
    public boolean add(E e) {
        return set.add(e) && randomizedList.add(e);
    }

    @Override
    public boolean remove(Object o) {
        return set.remove(o) && randomizedList.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return set.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return set.addAll(c) && randomizedList.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return set.removeAll(c) && randomizedList.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return set.retainAll(c) && randomizedList.retainAll(c);
    }

    @Override
    public void clear() {
        set.clear();
        randomizedList.clear();
    }

    @Override
    public E get(int index) {
        return randomizedList.get(index);
    }

    @Override
    public E set(int index, E element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(int index, E element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public E remove(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int indexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<E> listIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    public E getRandomElement() {
        return randomizedList.get(random.nextInt(randomizedList.size()));
    }

    public List<E> getRandomizedList(){
        return Collections.unmodifiableList(randomizedList);
    }

    public Set<E> getSet() {
        return Collections.unmodifiableSet(set);
    }

    public Random getRandom() {
        return random;
    }
}
