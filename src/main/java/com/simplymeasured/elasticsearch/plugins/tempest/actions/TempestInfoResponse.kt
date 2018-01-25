/*
 * The MIT License (MIT)
 * Copyright (c) 2017 DataRank, Inc.
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

package com.simplymeasured.elasticsearch.plugins.tempest.actions

import org.eclipse.collections.api.RichIterable
import org.eclipse.collections.api.map.MapIterable
import org.eclipse.collections.api.multimap.Multimap
import org.eclipse.collections.impl.factory.Maps
import org.eclipse.collections.impl.factory.Multimaps
import org.eclipse.collections.impl.factory.Sets
import org.eclipse.collections.impl.list.fixed.ArrayAdapter
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.ByteBufferStreamInput
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.StatusToXContentObject
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.rest.RestStatus
import org.joda.time.DateTime

class TempestInfoResponse: ActionResponse(), StatusToXContentObject {
    companion object {
        val CURRENT_MODEL_VERSION = 2
    }

    // ideally this should be a data class I think but Kotlin is requiring constructor parameters for that and the
    // default no-arg constructor just makes more sense here.
    var lastOptimalBalanceFoundDateTime: DateTime = DateTime(0)
    var lastBalanceChangeDateTime: DateTime = DateTime(0)
    var lastRebalanceAttemptDateTime: DateTime = DateTime(0)
    var lastNodeGroupScores: MapIterable<String, MapIterable<String, Double>> = Maps.immutable.empty()
    var youngIndexes: RichIterable<String> = Sets.mutable.empty<String>()
    var patternMapping: Multimap<String, String> = Multimaps.immutable.list.empty()

    var status: String = "unknown"

    override fun readFrom(inputStream: StreamInput) {
        inputStream.readByteArray()
                   .let { java.nio.ByteBuffer.wrap(it) }
                   .let(::ByteBufferStreamInput)
                   .apply { internalReadFrom(this) }
    }

    private fun internalReadFrom(inputStream: StreamInput) {
        // place holder for handling future model changes
        val modelVersion = inputStream.readVInt()
        this.lastOptimalBalanceFoundDateTime = DateTime(inputStream.readVLong())
        this.lastBalanceChangeDateTime = DateTime(inputStream.readVLong())
        this.lastRebalanceAttemptDateTime = DateTime(inputStream.readVLong())
        this.youngIndexes = Sets.mutable.of(*inputStream.readStringArray())
        this.patternMapping = readPatternMapping(inputStream)
        this.status = inputStream.readString()
        this.lastNodeGroupScores = readNodeGroupScores(inputStream)
    }

    private fun readPatternMapping(inputStream: StreamInput): Multimap<String, String> =
            Multimaps.mutable.list.empty<String, String>().apply {
                val numberOfItems = inputStream.readVInt()

                for (itemIndex in 1..numberOfItems) {
                    val pattern = inputStream.readString()
                    val mappings = inputStream.readStringArray()
                    this.putAll(pattern, ArrayAdapter.adapt(*mappings))
                }
            }


    private fun readNodeGroupScores(inputStream: StreamInput): MapIterable<String, MapIterable<String, Double>> {
        return Maps.mutable.empty<String, MapIterable<String, Double>>().apply {
            val numberOfItems = inputStream.readVInt()

            for (itemIndex in 1..numberOfItems) {
                val hostname = inputStream.readString()
                val groups = readGroupScores(inputStream)
                this.put(hostname, groups)
            }
        }
    }

    private fun readGroupScores(inputStream: StreamInput): MapIterable<String, Double> {
        return Maps.mutable.empty<String, Double>().apply {
            val numberOfItems = inputStream.readVInt()

            for (itemIndex in 1..numberOfItems) {
                val group = inputStream.readString()
                val score = inputStream.readDouble()
                this.put(group, score)
            }
        }
    }

    override fun writeTo(outputStream: StreamOutput) {
        outputStream.writeVInt(CURRENT_MODEL_VERSION)
        outputStream.writeVLong(lastOptimalBalanceFoundDateTime.millis)
        outputStream.writeVLong(lastBalanceChangeDateTime.millis)
        outputStream.writeVLong(lastRebalanceAttemptDateTime.millis)
        outputStream.writeStringArray(youngIndexes.toArray(emptyArray<String>()))
        writePatternMapping(outputStream, patternMapping)
        outputStream.writeString(status)
        writeNodeGroupScores(outputStream, lastNodeGroupScores)
    }

    private fun writePatternMapping(outputStream: StreamOutput, patternMapping: Multimap<String, String>) {
        outputStream.writeVInt(patternMapping.sizeDistinct())

        patternMapping.forEachKeyMultiValues { pattern, indexes ->
            outputStream.writeString(pattern)
            outputStream.writeStringArray((indexes as RichIterable<String>).toArray(emptyArray<String>()))
        }
    }

    private fun writeNodeGroupScores(
            outputStream: StreamOutput,
            groupScores: MapIterable<String, MapIterable<String, Double>>) {
        outputStream.writeVInt(groupScores.size())

        groupScores.forEachKeyValue { host, groups ->
            outputStream.writeString(host)
            writeGroupScores(outputStream, groups)
        }
    }

    private fun writeGroupScores(outputStream: StreamOutput, groups: MapIterable<String, Double>) {
        outputStream.writeVInt(groups.size())

        groups.forEachKeyValue { description, score ->
            outputStream.writeString(description)
            outputStream.writeDouble(score)
        }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.field("lastOptimalBalanceFoundDateTime", lastOptimalBalanceFoundDateTime)
        builder.field("lastBalanceChangeDateTime", lastBalanceChangeDateTime)
        builder.field("lastRebalanceAttemptDateTime", lastRebalanceAttemptDateTime)
        builder.startArray("youngIndexes")
        youngIndexes.forEach { builder.value(it) }
        builder.endArray()
        builder.field("patternMap", patternMapping.toMap() as Map<*, *>)
        builder.field("nodeGroupScores", lastNodeGroupScores as Map<*, *>)
        builder.field("status", status)
        return builder
    }

    override fun status(): RestStatus = RestStatus.OK
}