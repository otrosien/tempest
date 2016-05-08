package com.simplymeasured.elasticsearch.plugins.tempest.balancer

import org.eclipse.collections.impl.factory.SortedSets
import org.eclipse.collections.impl.set.sorted.mutable.TreeSortedSet
import java.util.*
import kotlin.comparisons.compareBy

/**
 * Created by awhite on 4/18/16.
 */
class MinimumNQueue<T>(val size: Int, val comparator: Comparator<T>) {
    constructor(size: Int, selector: (T) -> Comparable<*>) : this(size, compareBy(selector))

    val orderedSet: SortedSet<T> = TreeSortedSet(comparator)

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