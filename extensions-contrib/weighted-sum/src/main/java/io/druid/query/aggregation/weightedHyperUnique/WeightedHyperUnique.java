/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.weightedHyperUnique;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import java.nio.ByteBuffer;
import java.util.*;

public class WeightedHyperUnique {
    Double value;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WeightedHyperUnique that = (WeightedHyperUnique) o;

        if (Double.compare(that.value, value) != 0) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        return result;
    }


    public WeightedHyperUnique(double value) {
        this.value = value;
    }

    public WeightedHyperUnique() {
        this(0);
    }

    @Override
    public String toString() {
        return "WeightedHyperUnique{" +
                "value=" + value +
                '}';
    }

    /**
     * Adds the given value to the histogram
     *
     * @param value the value to be added
     */
    public void offer(float value) {
        this.value += value;
    }

    public WeightedHyperUnique fold(WeightedHyperUnique h) {
        this.value += h.value;
        return this;
    }

    /**
     * Returns a byte-array representation of this ApproximateHistogram object
     *
     * @return byte array representation
     */
    @JsonValue
    public byte[] toBytes() {
        ByteBuffer buf = ByteBuffer.allocate(Double.SIZE);
        toByteBuf(buf);
        return buf.array();
    }

    public void toByteBuf(ByteBuffer buffer){
        buffer.putDouble(value);
    }

    public static WeightedHyperUnique fromByteBuf(ByteBuffer buffer){
        return new WeightedHyperUnique(buffer.getDouble());
    }


    public int getMaxStorageSize() {
        return Double.SIZE;
    }

    /**
     * Returns the minimum number of bytes required to store this ApproximateHistogram object
     *
     * @return required number of bytes
     */
    public int getMinStorageSize() {
        return Double.SIZE;
    }

    public void toBytes(ByteBuffer buf) {
        buf.putDouble(value);
    }

    /**
     * Constructs an Approximate Histogram object from the given byte-array representation
     *
     * @param bytes byte array to construct an ApproximateHistogram from
     * @return ApproximateHistogram constructed from the given byte array
     */
    public static WeightedHyperUnique fromBytes(byte[] bytes) {
        return fromByteBuf(ByteBuffer.wrap(bytes));
    }

    public Float getValue() {
        return new Float(value);
    }
}
