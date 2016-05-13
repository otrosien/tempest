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