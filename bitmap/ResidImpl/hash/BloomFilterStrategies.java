/*
 * Copyright (C) 2011 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.bitmap.ResidImpl.hash;

import com.bitmap.ResidImpl.hash.BloomFilterStrategies.RedisBitArray;

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import redis.clients.jedis.Jedis;

import java.math.RoundingMode;
import java.util.concurrent.atomic.AtomicLongArray;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Collections of strategies of generating the k * log(M) bits required for an element to be mapped
 * to a BloomFilter of M bits and k hash functions. These strategies are part of the serialized form
 * of the Bloom filters that use them, thus they must be preserved as is (no updates allowed, only
 * introduction of new versions).
 * <p>
 * Important: the order of the constants cannot change, and they cannot be deleted - we depend on
 * their ordinal for BloomFilter serialization.
 *
 * @author Dimitris Andreou
 * @author Kurt Alfred Kluever
 */
enum BloomFilterStrategies implements BloomFilter.Strategy {
    /**
     * See "Less Hashing, Same Performance: Building a Better Bloom Filter" by Adam Kirsch and Michael
     * Mitzenmacher. The paper argues that this trick doesn't significantly deteriorate the
     * performance of a Bloom filter (yet only needs two 32bit hash functions).
     */
    MURMUR128_MITZ_32() {
        @Override
        public <T> boolean put(
                T object, Funnel<? super T> funnel, int numHashFunctions, RedisBitArray bits) {
            long bitSize = bits.bitSize();
            long hash64 = Hashing.murmur3_128().hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            boolean bitsChanged = false;
            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                bitsChanged |= bits.set(combinedHash % bitSize);
            }
            return bitsChanged;
        }

        @Override
        public <T> boolean mightContain(
                T object, Funnel<? super T> funnel, int numHashFunctions, RedisBitArray bits) {
            long bitSize = bits.bitSize();
            long hash64 = Hashing.murmur3_128().hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                if (!bits.get(combinedHash % bitSize)) {
                    return false;
                }
            }
            return true;
        }
    },
    /**
     * This strategy uses all 128 bits of {@link Hashing#murmur3_128} when hashing. It looks different
     * than the implementation in MURMUR128_MITZ_32 because we're avoiding the multiplication in the
     * loop and doing a (much simpler) += hash2. We're also changing the index to a positive number by
     * AND'ing with Long.MAX_VALUE instead of flipping the bits.
     */
    MURMUR128_MITZ_64() {
        @Override
        public <T> boolean put(
                T object, Funnel<? super T> funnel, int numHashFunctions, RedisBitArray bits) {
            long bitSize = bits.bitSize();
            byte[] bytes = Hashing.murmur3_128().hashObject(object, funnel).getBytesInternal();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            boolean bitsChanged = false;
            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                bitsChanged |= bits.set((combinedHash & Long.MAX_VALUE) % bitSize);
                combinedHash += hash2;
            }
            return bitsChanged;
        }

        @Override
        public <T> boolean mightContain(
                T object, Funnel<? super T> funnel, int numHashFunctions, RedisBitArray bits) {
            long bitSize = bits.bitSize();
            byte[] bytes = Hashing.murmur3_128().hashObject(object, funnel).getBytesInternal();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                if (!bits.get((combinedHash & Long.MAX_VALUE) % bitSize)) {
                    return false;
                }
                combinedHash += hash2;
            }
            return true;
        }

        private /* static */ long lowerEight(byte[] bytes) {
            return Longs.fromBytes(
                    bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        }

        private /* static */ long upperEight(byte[] bytes) {
            return Longs.fromBytes(
                    bytes[15], bytes[14], bytes[13], bytes[12], bytes[11], bytes[10], bytes[9], bytes[8]);
        }
    };

    /**
     * Models a lock-free array of bits.
     * <p>
     * <p>We use this instead of java.util.BitSet because we need access to the array of longs and we
     * need compare-and-swap.
     */
    static final class RedisBitArray {
        private static final int LONG_ADDRESSABLE_BITS = 6;
//    final AtomicLongArray data;
//    private final LongAddable bitCount;

        private String actKey;
        private String statType;

        Jedis jedis = new Jedis("localhost");

        RedisBitArray(long bits, String actKey, String statType) {
//        this(new long[Ints.checkedCast(LongMath.divide(bits, 64, RoundingMode.CEILING))], actKey, statType);
            int redisBits = Ints.checkedCast(LongMath.divide(bits, 64, RoundingMode.CEILING)) * 64;
            checkArgument(redisBits > 0, "data length is zero!");

            // the start index of redis's bitset is 0,the last index is size-1
            jedis.setbit(getKey(actKey, statType), redisBits - 1, false);

            this.actKey = actKey;
            this.statType = statType;
        }

//    // Used by serialization
//    RedisBitArray(long[] data, String actKey, String statType) {
//        checkArgument(data.length > 0, "data length is zero!");
//        this.data = new AtomicLongArray(data);
////      this.bitCount = LongAddables.create();
//        long bitCount = 0;
//        for (long value : data) {
//            bitCount += Long.bitCount(value);
//        }
//        this.actKey = actKey;
//        this.statType = statType;
////      this.bitCount.add(bitCount);
//    }

        /**
         * Returns true if the bit changed value, and ensure that the value is we need to set
         */
        boolean set(long bitIndex) {
//        if (get(bitIndex)) {
//            return false;
//        }

//        int longIndex = (int) (bitIndex >>> LONG_ADDRESSABLE_BITS);
//        long mask = 1L << bitIndex; // only cares about low 6 bits of bitIndex


//        long oldValue;
//        long newValue;
//        do {
////            oldValue = data.get(longIndex);
//            oldValue = jedis.getbit(longIndex);
//            newValue = oldValue | mask;
//            if (oldValue == newValue) {
//                return false;
//            }
//        } while (!data.compareAndSet(longIndex, oldValue, newValue));

            // We turned the bit on, so increment bitCount.
//      bitCount.increment();
//        再确认下是否与redis 命令一样，返回set 之间的值
            return !jedis.setbit(getKey(actKey, statType), bitIndex, true);
//        return true;
        }

        boolean get(long bitIndex) {
//        return (data.get((int) (bitIndex >>> 6)) & (1L << bitIndex)) != 0;
            return jedis.getbit(getKey(actKey, statType), bitIndex);
        }

        /**
         * Careful here: if threads are mutating the atomicLongArray while this method is executing, the
         * final long[] will be a "rolling snapshot" of the state of the bit array. This is usually good
         * enough, but should be kept in mind.
         */
        public static long[] toPlainArray(AtomicLongArray atomicLongArray) {
            long[] array = new long[atomicLongArray.length()];
            for (int i = 0; i < array.length; ++i) {
                array[i] = atomicLongArray.get(i);
            }
            return array;
        }

        /**
         * Number of bits
         */
        long bitSize() {
//        return (long) data.length() * Long.SIZE;
            return jedis.strlen(getKey(actKey, statType));
        }

        /**
         * Number of set bits (1s).
         * <p>
         * <p>Note that because of concurrent set calls and uses of atomics, this bitCount is a (very)
         * close *estimate* of the actual number of bits set. It's not possible to do better than an
         * estimate without locking. Note that the number, if not exactly accurate, is *always*
         * underestimating, never overestimating.
         */
        long bitCount() {
            return jedis.bitcount(getKey(actKey, statType));
        }

//    RedisBitArray copy() {
//        return new RedisBitArray(toPlainArray(data), actKey, statType);
//    }

        /**
         * Combines the two BitArrays using bitwise OR.
         * <p>
         * <p>NOTE: Because of the use of atomics, if the other RedisBitArray is being mutated while
         * this operation is executing, not all of those new 1's may be set in the final state of this
         * RedisBitArray. The ONLY guarantee provided is that all the bits that were set in the other
         * RedisBitArray at the start of this method will be set in this RedisBitArray at the end
         * of this method.
         */
//    void putAll(RedisBitArray other) {
//        checkArgument(
//                data.length() == other.data.length(),
//                "BitArrays must be of equal length (%s != %s)",
//                data.length(),
//                other.data.length());
//        for (int i = 0; i < data.length(); i++) {
//            long otherLong = other.data.get(i);
//
//            long ourLongOld;
//            long ourLongNew;
//            boolean changedAnyBits = true;
//            do {
//                ourLongOld = data.get(i);
//                ourLongNew = ourLongOld | otherLong;
//                if (ourLongOld == ourLongNew) {
//                    changedAnyBits = false;
//                    break;
//                }
//            } while (!data.compareAndSet(i, ourLongOld, ourLongNew));
//
//            if (changedAnyBits) {
//                int bitsAdded = Long.bitCount(ourLongNew) - Long.bitCount(ourLongOld);
//                bitCount.add(bitsAdded);
//            }
//        }
//    }

//    @Override
//    public boolean equals(@Nullable Object o) {
//        if (o instanceof RedisBitArray) {
//            RedisBitArray lockFreeBitArray = (RedisBitArray) o;
//            // TODO(lowasser): avoid allocation here
//            return Arrays.equals(toPlainArray(data), toPlainArray(lockFreeBitArray.data));
//        }
//        return false;
//    }
//
//    @Override
//    public int hashCode() {
//        // TODO(lowasser): avoid allocation here
//        return Arrays.hashCode(toPlainArray(data));
//    }
        private String getKey(String actKey, String statType) {
            return String.format("bloomFilter_act_%s_statType_%s", actKey, statType);
        }

    }
}
