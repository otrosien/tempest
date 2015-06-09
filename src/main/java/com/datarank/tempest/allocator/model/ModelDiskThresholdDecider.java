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
package com.datarank.tempest.allocator.model;

import org.elasticsearch.common.settings.Settings;

public class ModelDiskThresholdDecider extends ModelAllocationDecider {
    protected ModelDiskThresholdDecider(Settings settings) {
        super(settings);
    }

    /* TODO: Future enhancement: model this and respect watermark settings so that ModelCluster never tries to fork in a way that would
       violate disk watermark thresholds. Be sure to include the sizes of shards that have already been moved to a node
       or moved from a node in cluster.modelOperations so that we don't accidentally overfill a node by ignoring the time
       it takes to move a node. When in doubt, always assume the node is in the fuller state.
     */
    @Override
    public boolean canRelocate(ModelCluster cluster, ModelOperation operation) {
       return true;
    }
}
