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

import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.index.shard.ShardId;

public class ModelShard {
    public static final long UNKNOWN_SHARD_SIZE = 82; // an empty shard is 82 bytes

    private final MutableShardRouting routingShard;
    private final ShardId shardIdentifier; // unique references, equal values between primaries and replicas
    private final int id;
    private final long size;

    private boolean isReplica;
    private ModelShard primaryShard;

    public ModelShard(final int id, final long size) {
        this.routingShard = null;
        this.shardIdentifier = null;
        this.id = id;
        this.size = size;
        this.isReplica = false;
        this.primaryShard = null;
    }

    public ModelShard(final MutableShardRouting routingShard, final long size) {
        this.routingShard = routingShard;
        this.shardIdentifier = routingShard.shardId();
        this.id = routingShard.getId();
        this.size = size;
        this.isReplica = !routingShard.primary();
    }

    public MutableShardRouting getRoutingShard() {
        return routingShard;
    }

    public ShardId getShardIdentifier() {
        return shardIdentifier;
    }

    public int getId() { return id; }

    public long getSize() {
        return size;
    }

    public boolean isReplica(){
        return isReplica;
    }

    public ModelShard getPrimaryShard(){
        return this.primaryShard;
    }

    public void setIsReplica(boolean isReplica) {
        this.isReplica = isReplica;
    }

    public void setPrimaryShard(ModelShard primaryShard) {
        this.isReplica = this != primaryShard;
        this.primaryShard = primaryShard;
    }
}
