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
    private final List<E> list;
    private final Set<E> set;
    private final Random random;

    public RandomList(final Random random) {
        set = new HashSet<>();
        list = new ArrayList<>();
        this.random = random;
    }

    public RandomList(final int capacity, final Random random) {
        set = new HashSet<>(capacity);
        list = new ArrayList<>(capacity);
        this.random = random;
    }

    public RandomList(final RandomList other) {
        set = new HashSet<>(other.getSet());
        list = new ArrayList<>(other.getList());
        random = other.getRandom();
    }

    public RandomList(final List other, final Random random) {
        set = new HashSet<>(other);
        list = new ArrayList<>(other);
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
        Collections.shuffle(list, random);
        return list.iterator();
    }

    @Override
    public Object[] toArray() {
        return list.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return list.toArray(a);
    }

    @Override
    public boolean add(E e) {
        return set.add(e) && list.add(e);
    }

    @Override
    public boolean remove(Object o) {
        return set.remove(o) && list.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return set.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return set.addAll(c) && list.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return set.removeAll(c) && list.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return set.retainAll(c) && list.retainAll(c);
    }

    @Override
    public void clear() {
        set.clear();
        list.clear();
    }

    @Override
    public E get(int index) {
        throw new UnsupportedOperationException();
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
        return list.get(random.nextInt(list.size()));
    }

    public List<E> getList(){
        return Collections.unmodifiableList(list);
    }

    public Set<E> getSet() {
        return Collections.unmodifiableSet(set);
    }

    public Random getRandom() {
        return random;
    }
}
