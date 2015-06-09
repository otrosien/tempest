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

/**
 * Represents a forking operation on a cluster. ModelOperations always represent a move to a node, from another node
 * or from unassigned.
 */
public class ModelOperation {
    public final ModelShard modelShard;
    public final ModelNode sourceNode;
    public final ModelContainerType sourceType;
    public ModelNode destinationNode;

    public static final ModelOperation NO_OP = new ModelOperation(null, null, null);

    /**
     * Move a shard to a node
     * @param shard
     * @param source
     * @param destination
     */
    public ModelOperation (ModelShard shard, ModelNode source, ModelNode destination) {
        sourceType = source == null ? ModelContainerType.UNASSIGNED : ModelContainerType.NODE;
        modelShard = shard;
        sourceNode = source;
        destinationNode = destination;
    }

    /**
     * Ensures that the correct members of operation are set or null based on its sourceType and destinationType
     * @param operation
     * @return true if the operation is valid
     */
    public static boolean isValid(ModelOperation operation) {
        if (operation == NO_OP) { return true; }
        if (operation.sourceNode != null && operation.sourceNode == operation.destinationNode) {
            // can't move a shard to the node it's already on
            return false;
        }

        boolean valid = operation.destinationNode != null;
        switch (operation.sourceType) {
            case UNASSIGNED:
                valid &= operation.sourceNode == null;
                break;
            case NODE:
                valid &= operation.sourceNode != null;
                break;
            default:
                throw new IllegalStateException("Invalid sourceType " + operation.sourceType + " for operation.");
        }
        return valid;
    }

}

