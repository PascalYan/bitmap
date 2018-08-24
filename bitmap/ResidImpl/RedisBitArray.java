package com.bitmap.ResidImpl;

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import javax.annotation.Nullable;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Models a redis array of bits.
 * <p>
 */
final class RedisBitArray {
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