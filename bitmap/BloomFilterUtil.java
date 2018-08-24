package com.bitmap;

import redis.clients.jedis.Jedis;


import javax.annotation.Resource;
import java.util.HashSet;
import java.util.Set;

public class BloomFilterUtil {
    private final String REDIS_KEY = "boomfilter";
    private int numFunction;

    public void setNumFunction(int numFunction) {
        this.numFunction = numFunction;
    }

//    @Resource
//    private R2mClusterClient statR2mClusterClient;
    private Jedis jedis = new Jedis("localhost");

    /**
     * 判断用户pin是否加入过集合，如果没有加入过则加入
     *
     * @param key
     * @return
     */
    public boolean isExist (String key) throws Exception{
        int[] indexs = getIndexs(key);
        boolean result = true;
        for (int index : indexs) {
            if (!jedis.getbit(REDIS_KEY, index)) {
                result = false;
            }
        }
        if (!result) {
            add(key);
        }
        return result;
    }

    private void add(String key) {
        int[] indexs = getIndexs(key);
        for (int index : indexs) {
            Boolean result = jedis.setbit(REDIS_KEY, index, true);
            System.out.println();
        }
    }

    /**
     * 根据
     *
     * @param key
     * @return
     */
    private int[] getIndexs(String key) {
        int hash1 = hash(key);
        int hash2 = hash1 >>> 16;
        int[] result = new int[numFunction];
        for (int i = 0; i < numFunction; i++) {
            int nextHash = hash1 + i * hash2;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            result[i] = nextHash;
        }
        return result;
    }

    /**
     * 获取一个hash值
     *
     * @param key 用户pin
     * @return
     */
    private int hash(String key) {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }

    public void test(int size) {
        //statR2mClusterClient.del(REDIS_KEY);
        Long bitcount = jedis.bitcount(REDIS_KEY);

        Set<String> set = new HashSet<String>(size);
        String key;

        System.out.println("==================================================");
        for (int i = 0; i < size; i++) {
            if (i%5==0){
                key = "55555";
            }else {
                key = AppKeyGenerator.generate("sdsd" + i);
            }
            boolean s = !set.add(key);
            boolean b;
            try {
                b = isExist(key);
            }catch (Exception e){
                continue;
            }

            System.out.print(i + " key: " + key + " Set:" + s + " BloomFilter: " + b);
            if (s != b) {
                System.err.print("Error!");
            }
            System.out.println();
        }
        System.out.println("==================================================");
    }

    public static void main(String[] args) {
        new  BloomFilterUtil().test(10);
    }
}