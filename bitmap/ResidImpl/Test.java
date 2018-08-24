package com.bitmap.ResidImpl;

import com.bitmap.AppKeyGenerator;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by admin on 17/7/7. * 布隆过滤器
 */
public class Test {
    private static int size = 100000;
    private static BloomFilter<String> bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), size);
//    private static BloomFilter<String> bloomFilter = BloomFilter.create(Funnels.integerFunnel(), size);

    public static void main(String[] args) {
        for (int i = 0; i < size; i++) {
            bloomFilter.put(AppKeyGenerator.generate("xax" + i));
        }
        for (int i = 0; i < size; i++) {
            if (!bloomFilter.mightContain(AppKeyGenerator.generate("xax" + i))) {
                System.out.println("有坏人逃脱了");
            }
        }
        List<Integer> list = new ArrayList<Integer>(1000);
        for (int i = size + 10000; i < size + 20000; i++) {
            if (bloomFilter.mightContain(AppKeyGenerator.generate("xax" + i))) {
                list.add(i);
            }
        }
        System.out.println("有误伤的数量：" + list.size());
    }
}