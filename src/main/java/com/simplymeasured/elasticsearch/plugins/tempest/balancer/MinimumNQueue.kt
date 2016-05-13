/*
 * The MIT License (MIT)
 * Copyright (c) 2016 DataRank, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 *
 */

package com.simplymeasured.elasticsearch.plugins.tempest.balancer

import org.eclipse.collections.impl.factory.SortedSets
import org.eclipse.collections.impl.set.sorted.mutable.TreeSortedSet
import java.util.*
import kotlin.comparisons.compareBy

/**
 * Generic queue that tracks the top N items
 */
class MinimumNQueue<T>(val size: Int, val comparator: Comparator<T>) {
    constructor(size: Int, selector: (T) -> Comparable<*>) : this(size, compareBy(selector))

    val orderedSet: SortedSet<T> = TreeSortedSet(comparator)

    /**
     * Attempt to add an item to the queue
     *
     * If the item can be added to the queue then the worst item is removed
     *
     * @return true if the item was added else false
     */
    fun tryAdd(item: T): Boolean {
        if (orderedSet.contains(item)) { return false }

        if (orderedSet.size < size) {
            orderedSet.add(item)
            return true;
        }

        val worstValue = orderedSet.last()

        if (comparator.compare(item, worstValue) >= 0) { return false }

        orderedSet.add(item)
        orderedSet.remove(worstValue)
        return true
    }

    fun asList(): List<T> = orderedSet.toList()
}