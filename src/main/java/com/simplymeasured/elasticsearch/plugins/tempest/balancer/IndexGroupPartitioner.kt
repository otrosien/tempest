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

import com.simplymeasured.elasticsearch.plugins.tempest.TempestConstants
import org.eclipse.collections.api.RichIterable
import org.eclipse.collections.api.list.ListIterable
import org.eclipse.collections.api.map.MapIterable
import org.eclipse.collections.api.multimap.Multimap
import org.eclipse.collections.impl.factory.Lists
import org.eclipse.collections.impl.list.mutable.FastList
import org.eclipse.collections.impl.tuple.Tuples
import org.eclipse.collections.impl.utility.LazyIterate
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.common.component.AbstractComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.settings.ClusterSettings
import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.settings.Setting.Property.*
import org.elasticsearch.common.settings.Settings
import java.util.regex.Pattern
import java.util.regex.PatternSyntaxException
import javax.swing.UIManager.put

/**
 * Partition indexes into groups based on a user defined regular expression
 */
open class IndexGroupPartitioner
@Inject constructor(
        settings: Settings,
        clusterSettings: ClusterSettings) : AbstractComponent(settings) {

    companion object {
        var INDEX_GROUP_PATTERN_SETTING: Setting<String> = Setting.simpleString(TempestConstants.GROUPING_PATTERNS, NodeScope, Dynamic)
    }

    // commas aren't perfect here since they can legally be defined in regexes but it seems reasonable for now;
    // perhaps there is a more generic way to define groups
    private var indexGroupPatternSettingValue: String = ".*"
        set (value) {
            field = value
            indexPatterns = field
                    .split(",")
                    .mapNotNull { safeCompile(it) }
                    .let { Lists.mutable.ofAll(it) }
                    .apply { this.add(safeCompile(".*")) }
                    .toImmutable()
        }

    private var indexPatterns: ListIterable<Pattern> = Lists.immutable.of(safeCompile(".*"))

    init {
        clusterSettings.addSettingsUpdateConsumer(INDEX_GROUP_PATTERN_SETTING, this::indexGroupPatternSettingValue.setter)
        indexGroupPatternSettingValue = INDEX_GROUP_PATTERN_SETTING.get(settings)
    }

    private fun safeCompile(it: String): Pattern? {
        return try {
            Pattern.compile(it)
        } catch(e: PatternSyntaxException) {
            logger.warn("failed to compile group pattern ${it}")
            null
        }
    }

    fun partition(metadata: MetaData): RichIterable<RichIterable<IndexMetaData>> = FastList
            .newWithNValues(indexPatterns.size()) { Lists.mutable.empty<IndexMetaData>() }
            .apply { metadata.forEach { this[determineGroupNumber(it.index.name)].add(it) } }
            .let { it as RichIterable<RichIterable<IndexMetaData>> }

    fun patternMapping(metadata: MetaData) : Multimap<String, String> = LazyIterate
            .adapt(metadata)
            .collect { it.index.name }
            .groupBy { indexPatterns[determineGroupNumber(it)].toString() }

    private fun determineGroupNumber(indexName: String): Int {
        return indexPatterns.indexOfFirst { it.matcher(indexName).matches() }
    }
}