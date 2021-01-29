package com.github.mgunlogson.cuckoofilter4j;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Funnel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class CountingCuckooFilter<T> implements Serializable {

    private static long numBuckets;

    /*
     * IMPORTANT THREAD SAFETY NOTES. To prevent deadlocks, all methods needing
     * multiple locks need to lock the victim first. This is followed by the
     * segment locks, which need to be locked in ascending order of segment in
     * the backing lock array. The bucketlocker will always lock multiple
     * buckets in the same order if you use it properly.
     *
     */
    private static final long serialVersionUID = -1337735144654851942L;
    static final int INSERT_ATTEMPTS = 500;
    static final int BUCKET_SIZE = 4;
    // make sure to update getNeededBitsForFpRate() if changing this... then
    // again don't change this
    private static final double LOAD_FACTOR = 0.955;
    private static final double DEFAULT_FP = 0.01;
    private static final int DEFAULT_CONCURRENCY = 16;

    @VisibleForTesting
    final FilterTable table;
    @VisibleForTesting
    final IndexTagCalc<T> hasher;
    private final AtomicLong count;
    /**
     * Only stored for serialization since the bucket locker is transient.
     * equals() and hashcode() just check the concurrency value in the bucket
     * locker and ignore this
     */
    private final int expectedConcurrency;
   // private final StampedLock victimLock;
    private transient SegmentedBucketLocker bucketLocker;

    /* @VisibleForTesting
    Utils.Victim victim;
    @VisibleForTesting
    boolean hasVictim;*/

    List<long[]> victims = Collections.synchronizedList(new ArrayList<long[]>());


    /**
     * Creates a Cuckoo filter.
     */
    private CountingCuckooFilter(IndexTagCalc<T> hasher, FilterTable table, AtomicLong count, int expectedConcurrency) {
        this.hasher = hasher;
        this.table = table;
        this.count = count;

        this.expectedConcurrency = expectedConcurrency;
        this.bucketLocker = new SegmentedBucketLocker(expectedConcurrency);
    }

    /***
     * Builds a Cuckoo Filter. To Create a Cuckoo filter, construct this then
     * call {@code #build()}.
     *
     * @author Mark Gunlogson
     *
     * @param <T>
     *            the type of item {@code Funnel will use}
     */
    public static class Builder<T> {
        // required arguments
        private final Funnel<? super T> funnel;
        private final long maxKeys;
        // optional arguments
        private Utils.Algorithm hashAlgorithm;
        private double fpp = DEFAULT_FP;
        private int expectedConcurrency = DEFAULT_CONCURRENCY;



        public Builder(Funnel<? super T> funnel, long maxKeys) {
            checkArgument(maxKeys > 1, "maxKeys (%s) must be > 1, increase maxKeys", maxKeys);
            checkNotNull(funnel);
            this.funnel = funnel;
            this.maxKeys = maxKeys;
        }

        public Builder(Funnel<? super T> funnel, int maxKeys) {
            this(funnel, (long) maxKeys);
        }

        public CountingCuckooFilter.Builder<T> withFalsePositiveRate(double fpp) {
            checkArgument(fpp > 0, "fpp (%s) must be > 0, increase fpp", fpp);
            checkArgument(fpp < .25, "fpp (%s) must be < 0.25, decrease fpp", fpp);
            this.fpp = fpp;
            return this;
        }

        public CountingCuckooFilter.Builder<T> withHashAlgorithm(Utils.Algorithm hashAlgorithm) {
            checkNotNull(hashAlgorithm,
                    "hashAlgorithm cannot be null. To use default, build without calling this method.");
            this.hashAlgorithm = hashAlgorithm;
            return this;
        }

        public CountingCuckooFilter.Builder<T> withExpectedConcurrency(int expectedConcurrency) {
            checkArgument(expectedConcurrency > 0, "expectedConcurrency (%s) must be > 0.", expectedConcurrency);
            checkArgument((expectedConcurrency & (expectedConcurrency - 1)) == 0,
                    "expectedConcurrency (%s) must be a power of two.", expectedConcurrency);
            this.expectedConcurrency = expectedConcurrency;
            return this;
        }

        public CountingCuckooFilter<T> build() {
            int tagBits = Utils.getBitsPerItemForFpRate(fpp, LOAD_FACTOR);
            numBuckets = Utils.getBucketsNeeded(maxKeys, LOAD_FACTOR, BUCKET_SIZE);
            IndexTagCalc<T> hasher;
            if (hashAlgorithm == null) {
                hasher = IndexTagCalc.create(funnel, numBuckets, tagBits);
            } else
                hasher = IndexTagCalc.create(hashAlgorithm, funnel, numBuckets, tagBits);
            FilterTable filtertbl = FilterTable.create(tagBits, numBuckets);
            return new CountingCuckooFilter<>(hasher, filtertbl, new AtomicLong(0), expectedConcurrency);
        }
    }

    public long getCount() {
        // can return more than maxKeys if running above design limit!
        return count.get();
    }

    public double getLoadFactor() {
        return count.get() / (hasher.getNumBuckets() * (double) BUCKET_SIZE);
    }

    public long getActualCapacity() {
        return hasher.getNumBuckets() * BUCKET_SIZE;
    }

    public long getStorageSize() {
        return table.getStorageSize();
    }

    /**
     * 进行单次插入，插入成功返回null
     * **/
    long[] insertTagToBucket(long curIndex,long altIndex,long curTag){
        long vs[] = new long[3];
        long tagCount = table.countTag(curIndex,altIndex,curTag) + 1; //包含了待插入的那一个
        bucketLocker.lockBucketsWrite(curIndex, altIndex);
        try{
           if(tagCount <= 3){
               if(table.insertTagToBucket(curIndex,curTag) || table.insertTagToBucket(altIndex,curTag)){
                   return null;
               }
               else{
                   vs = table.swapRandomInBucket(altIndex,curTag);
               }
           }
           else if(tagCount == 4){
               table.removeTags(curIndex,curTag);
               table.removeTags(altIndex,curTag);
               if(table.insertTagBoxToBucket(curIndex,curTag,tagCount) || table.insertTagBoxToBucket(altIndex,curTag,tagCount)){
                   return null;
               }
               else {
                   vs = table.swapRandomInBucket(altIndex,curTag,tagCount);
               }
           }
           else{

               if(table.updateTagCount(curIndex,curTag,tagCount) || table.updateTagCount(altIndex,curTag,tagCount)){ }
               else{
                   //TODO:这里未处理越界，添加到list中
                   vs[0] = curTag;
                   saveVictims(curIndex,vs);
               }
               return null;
           }
        }finally {
            bucketLocker.unlockBucketsWrite(curIndex, altIndex);
        }

       return vs;
    }

    long[] insertTagBoxToBucket(long curIndex,long altIndex, long curTag,long count){

        long vs[] = new long[3];
        long tagCount = table.countTag(curIndex,altIndex,curTag); //包含了待插入的那一个
        bucketLocker.lockBucketsWrite(curIndex, altIndex);
        try{
            if(tagCount >=4){  //如果>=4说明已经存在tagBox，直接更新
                if(table.updateTagCount(curIndex,curTag,tagCount + count)||
                        table.updateTagCount(curIndex,altIndex,tagCount + count)){ }
                else{
                    //TODO:这里未处理越界，添加到list中
                    vs[0] = curTag;
                    vs[2] = count;
                    saveVictims(curIndex,vs);
                }
                return null;
            }else if(tagCount > 0 && tagCount <= 3){
                //清空，添加到现有的tagCount中
                table.removeTags(curIndex, curTag);
                table.removeTags(altIndex, curTag);
                count += tagCount;
            }
            //尝试插入
            if(table.insertTagBoxToBucket(curIndex,curTag,count) || table.insertTagBoxToBucket(altIndex,curTag,count)){
                return null;
            }
            //插入失败，踢出
            vs = table.swapRandomInBucket(altIndex,curTag,count);

        }finally {
            bucketLocker.unlockBucketsWrite(curIndex, altIndex);
        }
        return vs;
    }

    private void victimsHandler(long curIndex, long[] vs, int index){
        if(vs == null){ return; }
       // System.out.println(Long.toBinaryString(vs[0])+","+Long.toBinaryString(vs[1])+","+Long.toBinaryString(vs[2]));
        if(index >= INSERT_ATTEMPTS){
            saveVictims(curIndex,vs);
            return;
        }

        if(isTagBox(vs)){
            long[] tVs = insertTagBoxToBucket(curIndex,hasher.altIndex(curIndex,vs[0]),vs[0],vs[2]);
            victimsHandler(curIndex,tVs,index + 1);

        }else{
            for (int i = 0; i <3 ; i++) {
                if(vs[i]!=0L){
                    long[] tVs = insertTagToBucket(curIndex,hasher.altIndex(curIndex,vs[i]),vs[i]);
                    victimsHandler(curIndex,tVs,index + 1);
                }
            }
        }
    }

    private void saveVictims(long curIndex, long[] vs){
        if(isTagBox(vs)){
            long[]v = new long[4];
            v[0] = curIndex;
            v[1] = hasher.altIndex(curIndex,vs[0]);
            v[2] = vs[0];
            v[3] = vs[2];
            victims.add(v);
        }else{
            for (int i = 0; i <3 ; i++) {
                if(vs[i]!=0L){
                    long[]v = new long[4];
                    v[0] = curIndex;
                    v[1] = hasher.altIndex(curIndex,vs[i]);
                    v[2] = vs[i];
                    v[3] = 1;
                    victims.add(v);
                }
            }
        }
    }


    public boolean put(T item){
        BucketAndTag pos = hasher.generate(item);
        long curTag = pos.tag;
        long curIndex = pos.index;
        long altIndex = hasher.altIndex(curIndex, curTag);

        long[] vs = insertTagToBucket(curIndex,altIndex,curTag);

        //上方是从curTag踢出的
        victimsHandler(altIndex,vs,0);

        if(victims.size() > 0){
            return false;
        }else {
            return true;
        }
    }



    private boolean isTagBox(long[] tb){
        if(tb[0]!=0 && tb[1]==0 && tb[2]!=0){
            return true;
        }else{
            return false;
        }
    }

    public boolean mightContain(T item) {
        BucketAndTag pos = hasher.generate(item);
        long i1 = pos.index;
        long i2 = hasher.altIndex(pos.index, pos.tag);
        bucketLocker.lockBucketsRead(i1, i2);
        try {
            // return table.findTag(i1,pos.tag) || table.findTag(i2,pos.tag) ;
            if (table.findTag(i1, pos.tag) || table.findTag(i2, pos.tag)) {
                return true;
            }
        } finally {
            bucketLocker.unlockBucketsRead(i1, i2);
        }
        // return checkIsVictim(pos);
        return false;
    }

    public boolean delete(T item){
        BucketAndTag pos = hasher.generate(item);
        long i1 = pos.index;
        long i2 = hasher.altIndex(pos.index, pos.tag);
        bucketLocker.lockBucketsWrite(i1, i2);
        try {
            if(table.deleteFromBucket(i1,pos.tag) || table.deleteFromBucket(i2,pos.tag)){
                count.decrementAndGet();
                return true;
            }
        } finally {
            bucketLocker.unlockBucketsWrite(i1, i2);
        }
        return false;
    }


    public void printMemBlock(String title){
        System.out.println("-------"+title +"---------");
        for(long i = 0; i < numBuckets; i++){
            for (int j = 0; j < CountingCuckooFilter.BUCKET_SIZE; j++) { //BUCKET_SIZE = 4
                System.out.print(Long.toBinaryString(table.readTag(i,j))+",");
            }
            System.out.println();
        }
        System.out.println("-----------------------------");
    }

    public void printMemBlock(){
        printMemBlock("");
    }
}
