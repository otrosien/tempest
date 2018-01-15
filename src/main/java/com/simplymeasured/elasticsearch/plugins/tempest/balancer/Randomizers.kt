/*
 * The MIT License (MIT)
 * Copyright (c) 2018 DataRank, Inc.
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

import org.eclipse.collections.api.RichIterable
import org.eclipse.collections.api.list.ListIterable
import org.eclipse.collections.api.ordered.OrderedIterable
import org.eclipse.collections.impl.list.mutable.FastList
import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList
import java.util.*

fun <T> ListIterable<T>.selectRandomWeightedElement(random: Random, weight: (T) -> Double): T? {
    if (this.isEmpty) { return null }
    val weights = DoubleArrayList.newWithNValues(this.size(), 0.0)
    this.forEachWithIndex { it, index ->
        val accumulator = if (index == 0) 0.0 else weights[index - 1]
        weights[index] = weight.invoke(it) + accumulator
    }

    val weightedSelection = random.nextDouble() * weights.last
    val selectedIndex = weights.binarySearch(weightedSelection).let { if (it < 0) -(it + 1) else it }
    return this[selectedIndex]
}

fun <T> ListIterable<T>.selectRandomNormalizedWeightedElement(random: Random, weight: (T) -> Double): T? {
    if (this.isEmpty) { return null }
    val accumulatedWeights = DoubleArrayList.newWithNValues(this.size(), 0.0)
    val maxWeight = this.asLazy().collect(weight).max()

    this.forEachWithIndex { it, index ->
        val accumulator = if (index == 0) 0.0 else accumulatedWeights[index - 1]
        accumulatedWeights[index] = maxWeight + 1.0 - weight.invoke(it) + accumulator
    }

    val weightedSelection = random.nextDouble() * accumulatedWeights.last
    val selectedIndex = accumulatedWeights.binarySearch(weightedSelection).let { if (it < 0) -(it + 1) else it }
    return this[selectedIndex]
}

fun <T> ListIterable<T>.selectRandomElement(random: Random): T {
    return this[random.nextInt(this.size())]
}