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

import com.simplymeasured.elasticsearch.plugins.tempest.actions.TempestInfoResponse.Companion.CURRENT_MODEL_VERSION
import org.eclipse.collections.api.RichIterable
import org.eclipse.collections.api.map.MapIterable
import org.eclipse.collections.api.set.SetIterable
import org.eclipse.collections.impl.factory.Maps
import org.eclipse.collections.impl.factory.Sets
import org.eclipse.collections.impl.list.fixed.ArrayAdapter
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.ByteBufferStreamInput
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.StatusToXContent
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.rest.RestStatus
import org.joda.time.DateTime

class TempestInfoResponse: ActionResponse(), StatusToXContent {
    companion object {
        val CURRENT_MODEL_VERSION = 1
    }

    // ideally this should be a data class I think but Kotlin is requiring constructor parameters for that and the
    // default no-arg constructor just makes more sense here.
    var lastOptimalBalanceFoundDateTime: DateTime = DateTime(0)
    var lastBalanceChangeDateTime: DateTime = DateTime(0)
    var lastRebalanceAttemptDateTime: DateTime = DateTime(0)
    var youngIndexes: SetIterable<String> = Sets.mutable.empty<String>()
    var patternMapping: MapIterable<String, RichIterable<String>> = Maps.mutable.empty()
    var status: String = "unknown"

    override fun readFrom(inputStream: org.elasticsearch.common.io.stream.StreamInput) {
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
    }


    private fun readPatternMapping(inputStream: StreamInput): MapIterable<String, RichIterable<String>> {
        return Maps.mutable.empty<String, RichIterable<String>>().apply {
            val numberOfItems = inputStream.readVInt()

            for (itemIndex in 1..numberOfItems) {
                val pattern = inputStream.readString()
                val mappings = inputStream.readStringArray()
                this.put(pattern, ArrayAdapter.adapt(*mappings))
            }
        }
    }

    override fun writeTo(outputStream: StreamOutput) {
        BytesStreamOutput()
                .apply { internalWriteTo(this, this@TempestInfoResponse) }
                .bytes()
                .apply { outputStream.writeByteArray(this.array()) }
    }

    private fun internalWriteTo(outputStream: StreamOutput, response: TempestInfoResponse) {
        outputStream.writeVInt(CURRENT_MODEL_VERSION)
        outputStream.writeVLong(response.lastOptimalBalanceFoundDateTime.millis)
        outputStream.writeVLong(response.lastBalanceChangeDateTime.millis)
        outputStream.writeVLong(response.lastRebalanceAttemptDateTime.millis)
        outputStream.writeStringArray(response.youngIndexes.toArray(emptyArray<String>()))
        writePatternMapping(outputStream, response.patternMapping)
        outputStream.writeString(response.status)
    }

    private fun writePatternMapping(outputStream: StreamOutput, patternMapping: MapIterable<String, RichIterable<String>>) {
        outputStream.writeVInt(patternMapping.size())

        patternMapping.forEachKeyValue { pattern, indexes ->
            outputStream.writeString(pattern)
            outputStream.writeStringArray(indexes.toArray(emptyArray<String>()))
        }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.field("lastOptimalBalanceFoundDateTime", lastOptimalBalanceFoundDateTime)
        builder.field("lastBalanceChangeDateTime", lastBalanceChangeDateTime)
        builder.field("lastRebalanceAttemptDateTime", lastRebalanceAttemptDateTime)
        builder.startArray("youngIndexes")
        youngIndexes.forEach { builder.value(it) }
        builder.endArray()
        builder.field("patternMap", patternMapping as Map<*, *>)
        builder.field("status", status)
        return builder
    }

    override fun status(): RestStatus = RestStatus.OK
}