/*
   Copyright 2016 Mark Gunlogson

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.github.mgunlogson.cuckoofilter4j;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;


import javax.annotation.Nullable;

import com.google.common.math.IntMath;
import com.google.common.math.LongMath;

/**
 * This class represents the link to access methods on the underlying BitSet.
 *
 * @author Mark Gunlogson
 *
 */
final class FilterTable implements Serializable {
	private static final long serialVersionUID = 4172048932165857538L;
	/*
	 * NOTE: Google's Guava library uses a custom BitSet implementation that
	 * looks to be adapted from the Lucene project. Guava project notes show
	 * this seems to be done for faster serialization and support for
	 * longs(giant filters). We just use the Lucene LongBitSet directly to make
	 * updates easier.
	 *
	 * NOTE: for speed, we don't check for inserts into invalid bucket indexes
	 * or bucket positions!
	 */
	private final LongBitSet memBlock;
	private final int bitsPerTag;
	private final long numBuckets;

	//进一步，如果我们默认tagBox模式下基数为2，考虑到0也算一种情况，则最多可存储3+2^bitsPerTag-1
	private final int maxTagCount;

	private FilterTable(LongBitSet memBlock, int bitsPerTag, long numBuckets) {
		this.bitsPerTag = bitsPerTag;
		this.memBlock = memBlock;
		this.numBuckets = numBuckets;
		this.maxTagCount = (int)(Math.pow(2,bitsPerTag))+2;
	}

	/**
	 * Creates a FilterTable
	 *
	 * @param bitsPerTag
	 *            number of bits needed for each tag
	 * @param numBuckets
	 *            number of buckets in filter
	 * @return
	 */
	static FilterTable create(int bitsPerTag, long numBuckets) {
		// why would this ever happen?
		checkArgument(bitsPerTag < 48, "tagBits (%s) should be less than 48 bits", bitsPerTag);
		// shorter fingerprints don't give us a good fill capacity
		checkArgument(bitsPerTag > 4, "tagBits (%s) must be > 4", bitsPerTag);
		checkArgument(numBuckets > 1, "numBuckets (%s) must be > 1", numBuckets);
		// checked so our implementors don't get too.... "enthusiastic" with
		// table size
		long bitsPerBucket = IntMath.checkedMultiply(CountingCuckooFilter.BUCKET_SIZE, bitsPerTag);
		long bitSetSize = LongMath.checkedMultiply(bitsPerBucket, numBuckets);
		LongBitSet memBlock = new LongBitSet(bitSetSize);
		return new FilterTable(memBlock, bitsPerTag, numBuckets);
	}

	long getStorageSize() {
		// NOTE: checked source in current Lucene LongBitSet class for thread
		// safety, make sure it stays this way if you update the class.
		return memBlock.length();
	}


	boolean insertTagToBucket(long bucketIndex, long tag) {
		for (int i = 0; i < CountingCuckooFilter.BUCKET_SIZE;) {
			if (!checkTag(bucketIndex, i, 0)){
				if(checkTagBox(bucketIndex,i)) i += 3;
				else i++;
			}else{
				writeTagNoClear(bucketIndex, i, tag);
				return true;
			}
		}
		return false;
	}

	/**
	 * 插入时，前面为tagBox，后面为tag.
	 * **/
	boolean insertTagBoxToBucket(long bucketIndex, long tag,long count){
		//从后往前统计是否有3个0；
		for(int i = CountingCuckooFilter.BUCKET_SIZE - 3; i < CountingCuckooFilter.BUCKET_SIZE;i++){
			if(!checkTag(bucketIndex,i,0)) return false;
		}
		createTagBox(bucketIndex,tag,count);
		return true;
	}

	//从头部添加tagbox，会挤出后三个
	void createTagBox(long bucketIndex, long tag, long count){
		if(count > maxTagCount){
			System.out.println("create fail: Tag count out of maxTagCount!");
		}
		for(int i = 0; i < 3; i++){
			moveTagBack(bucketIndex,i);
		}
		writeTagNoClear(bucketIndex,0,tag);//写入tag
		writeTagNoClear(bucketIndex,2,count - 3);//写入tagCount
	}

	/**
	 * update tagCount前需要保证是一个tagBox
	 * **/
	boolean updateTagCount(long bucketIndex, long tag, long count){
		if(count > maxTagCount){
			System.out.println("updateTagCount fail: Tag count out of maxTagCount!");
			return false;
		}
		for (int i = 0; i < CountingCuckooFilter.BUCKET_SIZE - 2; i++) {
			if(checkTag(bucketIndex,i,tag)){
				clearTagAndSet(bucketIndex,i+2,count - 3);
				return true;
			}
			else if(checkTag(bucketIndex,i,0))
				i++; //遇到0，跳过后面的count，防止count和tag冲突
		}
		return false;
	}

	boolean deleteFromBucket(long bucketIndex, long tag){
		for (int i = 0; i < CountingCuckooFilter.BUCKET_SIZE; i++) {
			if(checkTag(bucketIndex,i,tag)){
				if(checkTagBox(bucketIndex,i)){
					long count = readTag(bucketIndex,i+2);
					if(count == 1L){
						clearTagAndSet(bucketIndex,i,tag);
						clearTagAndSet(bucketIndex,i+1,tag);
						clearTagAndSet(bucketIndex,i+2,tag);
					}else{
						clearTagAndSet(bucketIndex,i+2,count - 1);
					}
				}else{
					moveTagForward(bucketIndex,i);
				}
				return true;
			}else if(checkTag(bucketIndex,i,0)) i++;
		}
		return false;
	}

	//只能删除tag对象，tagBox不能删除！
	/**
	 *  清空对象，后续补充
	 * **/
	void removeTags(long bucketIndex,long tag) {
		for (int i = 0; i < CountingCuckooFilter.BUCKET_SIZE;i++) {
			if (checkTag(bucketIndex, i, tag)) {
				moveTagForward(bucketIndex, i);
				i--;
			}
			else if (checkTag(bucketIndex, i, 0)) i++; //遇到0跳过count

		}
	}


	boolean findTag(long i1, long tag ) {
		for (int i = 0; i < CountingCuckooFilter.BUCKET_SIZE; i++) {
			if (checkTag(i1, i, tag)) return true;
			else if (checkTag(i1,i,0)) i++; //遇到0跳过count
		}
		return false;
	}
	
	/**
	 * 随机踢出一个tag或tagbox.
	 * 对一个tag而言，由于已经执行了插入操作，且插入失败，说明bucket中已经没有空的空间了，此时需要踢出一个tag或tagBox
	 * 对于tagBox而言，可能存在空间已满,需要踢出一个tagBox或3个tag
	 * 					还有1/2个空间,需要踢出2个或1个tag
	 * **/
	long [] swapRandomInBucket(long bucketIndex, long tag){
		int tagBoxNum = getTagBoxNum(bucketIndex);
		int rPos = ThreadLocalRandom.current().nextInt(CountingCuckooFilter.BUCKET_SIZE - (tagBoxNum*2));

		long victims[] = new long[3];
		if(rPos >= tagBoxNum){//为tag直接替换
			//(rPos - tagBoxNum) + (tagBoxNum * 3)
			victims[0] = readTagAndSet(bucketIndex,rPos+(tagBoxNum * 2),tag);
		}else{
			victims = readTagBoxAndDelete(bucketIndex,rPos*3);
			//insertTagToBucket(bucketIndex,tag);
			//由于插入时是满的，而我们又删掉了一个tagBox,所以可以直接插入到倒数第3个位置
			writeTagNoClear(bucketIndex,CountingCuckooFilter.BUCKET_SIZE-3,tag);
		}
		return victims;
	}

	/**
	 * tagBox首先会尝试直接插入，挤出1-3个tag,如果直接插入失败，则会尝试踢掉一个tagBox.
	 * **/
	long [] swapRandomInBucket(long bucketIndex, long tag, long count){

		long victims[] = new long[3];
		//检查是否有3个单位的空间,如果剩余空间小于3个，则无法开辟
		if(CountingCuckooFilter.BUCKET_SIZE - (getTagBoxNum(bucketIndex) * 3) >= 3 ){
			for (int i = 0; i < 3; i++) {	//直接读取最后3个tag
				victims[i] = readTag(bucketIndex,CountingCuckooFilter.BUCKET_SIZE-3+i);
			}
			createTagBox(bucketIndex,tag,count);//此时会踢掉最后3个；
			return victims;
		}

		//bucket_size = 7,则可存放的tagbox的个数为2 = bucket_size / 3
		int rPos = ThreadLocalRandom.current().nextInt(CountingCuckooFilter.BUCKET_SIZE / 3) * 3;

		victims[0] = readTagAndSet(bucketIndex,rPos,tag);
		//读取的tagCount需要+3
		victims[2] = readTagAndSet(bucketIndex,rPos+2,count - 3) + 3;
		return victims;
	}

	//获取tagBoxNum;
	int getTagBoxNum(long bucketIndex){
		int tagBoxNum = 0;
		for (int i = 0; i < CountingCuckooFilter.BUCKET_SIZE - 2; ) {
			if(checkTagBox(bucketIndex,i)){
				tagBoxNum++;
				i+=3;
			}else{ return tagBoxNum; } //因为tagBox都在前面，所以一旦非tagBox就可以返回。
		}
		return tagBoxNum;
	}


	/**
	 * Works but currently only used for testing
	 */
	long readTag(long bucketIndex, int posInBucket) {
		long tagStartIdx = getTagOffset(bucketIndex, posInBucket);
		long tag = 0;
		long tagEndIdx = tagStartIdx + bitsPerTag;
		// looping over true bits per nextBitSet javadocs
		for (long i = memBlock.nextSetBit(tagStartIdx); i >= 0 && i < tagEndIdx; i = memBlock.nextSetBit(i + 1L)) {
			// set corresponding bit in tag
			tag |= 1 << (i - tagStartIdx);
		}
		return tag;
	}

	long[] readTagBoxAndDelete(long bucketIndex,int tagPosInBucket){
		long tagBox[] = new long[3];
		for (int i = 0; i < 3; i++) {
			tagBox[i] = readTag(bucketIndex,tagPosInBucket);
			moveTagForward(bucketIndex,tagPosInBucket);
		}
		tagBox[2] += 3;
		return tagBox;
	}

	/**
	 * Writes a tag to a bucket position. Faster than regular write because it
	 * assumes tag starts with all zeros, but doesn't work properly if the
	 * position wasn't empty.
	 */
	void writeTagNoClear(long bucketIndex, int posInBucket, long tag) {
		long tagStartIdx = getTagOffset(bucketIndex, posInBucket);
		// BIT BANGIN YEAAAARRHHHGGGHHH
		for (int i = 0; i < bitsPerTag; i++) {
			// second arg just does bit test in tag
			if ((tag & (1L << i)) != 0) {
				memBlock.set(tagStartIdx + i);
			}
		}
	}

	void clearTagAndSet(long bucketIndex, int posInBucket, long newTag){
		long tagStartIdx = getTagOffset(bucketIndex, posInBucket);

		long tagEndIdx = tagStartIdx + bitsPerTag;
		int tagPos = 0;
		for (long i = tagStartIdx; i < tagEndIdx; i++) {
			if ((newTag & (1L << tagPos)) != 0) {
				memBlock.set(i);
			} else {
				memBlock.clear(i);
			}
			tagPos++;
		}
	}

	/**
	 * Reads a tag and sets the bits to a new tag at same time for max
	 * speedification
	 */
	long readTagAndSet(long bucketIndex, int posInBucket, long newTag) {
		long tagStartIdx = getTagOffset(bucketIndex, posInBucket);
		long tag = 0;
		long tagEndIdx = tagStartIdx + bitsPerTag;
		int tagPos = 0;
		for (long i = tagStartIdx; i < tagEndIdx; i++) {
			if ((newTag & (1L << tagPos)) != 0) {
				if (memBlock.getAndSet(i)) {
					tag |= 1 << tagPos;
				}
			} else {
				if (memBlock.getAndClear(i)) {
					tag |= 1 << tagPos;
				}
			}
			tagPos++;
		}
		return tag;
	}

	/**
	 * Check if a tag in a given position in a bucket matches the tag you passed
	 * it. Faster than regular read because it stops checking if it finds a
	 * non-matching bit.
	 */
	boolean checkTag(long bucketIndex, int posInBucket, long tag) {
		long tagStartIdx = getTagOffset(bucketIndex, posInBucket);
		final int bityPerTag = bitsPerTag;
		for (long i = 0; i < bityPerTag; i++) {
			if (memBlock.get(i + tagStartIdx) != ((tag & (1L << i)) != 0))
				return false;
		}
		return true;
	}

	boolean checkTagBox(long bucketIndex, int posInBucket) {
		if(posInBucket > (CountingCuckooFilter.BUCKET_SIZE - 3))
			return false;
		if(checkTag(bucketIndex,posInBucket+1,0)&&(!checkTag(bucketIndex,posInBucket+2,0)))
			return true;
		return false;
	}

	/**
	 * Similar to checkTag() except it counts the number of matches in the
	 * buckets.
	 */
	long countTag(long i1, long i2, long tag) {
		long tagCount = 0;
		for (int posInBucket = 0; posInBucket < CountingCuckooFilter.BUCKET_SIZE; posInBucket++) {
			if(checkTag(i1,posInBucket,tag)){
				if(checkTagBox(i1,posInBucket)) return readTag(i1,posInBucket+2) + 3;
				else tagCount++;
			}else if(checkTag(i1,posInBucket,0))
				posInBucket++;//跳过后面的
		}
		//TODO: i1,i2可能相等
		if(i1!=i2){
			for (int posInBucket = 0; posInBucket < CountingCuckooFilter.BUCKET_SIZE; posInBucket++) {
				if(checkTag(i2,posInBucket,tag)){
					if(checkTagBox(i2,posInBucket)) return readTag(i2,posInBucket+2) + 3;
					else tagCount++;
				}else if(checkTag(i2,posInBucket,0))
					posInBucket++;//跳过后面的
			}
		}
		return tagCount;
	}




	/**
	 *  Deletes (clears) a tag at a specific bucket index and position
	 *
	 * @param bucketIndex bucket index
	 * @param posInBucket position in bucket
	 */
	void clearTag(long bucketIndex, int posInBucket) {
		long tagStartIdx = getTagOffset(bucketIndex, posInBucket);
		memBlock.clear(tagStartIdx, tagStartIdx + bitsPerTag);
	}

	/**
	 * 往前移动一位
	 *
	 * 读取下一个tag,填充当前位置，最后一个tag置为0
	 * 	 **/
	void moveTagForward(long bucketIndex, int posInBucket){
		//long firstTag = readTag(bucketIndex,posInBucket);
		//System.out.println("bucketIndex is " + bucketIndex + ", posInBucket is "+posInBucket);
		clearTag(bucketIndex,posInBucket);
		for (int pos = posInBucket; pos < CountingCuckooFilter.BUCKET_SIZE - 1; pos++) {
			long tagStartIdx = getTagOffset(bucketIndex, pos) ;
			long tagEndIdx = tagStartIdx +  bitsPerTag;
			for (long i = tagStartIdx; i < tagEndIdx; i++) {
				if(memBlock.getAndClear(i + bitsPerTag))
					memBlock.set(i);
			}
		}
	}

	void moveTagBack(long bucketIndex, int posInBucket){
		clearTag(bucketIndex, CountingCuckooFilter.BUCKET_SIZE - 1); //删掉tag
		//long tag = readTagAndSet(bucketIndex,CountingCuckooFilter.BUCKET_SIZE - 1, 0);
		for (int pos = CountingCuckooFilter.BUCKET_SIZE - 1; pos > posInBucket ; pos--) {
			long tagStartIdx = getTagOffset(bucketIndex, pos) ;
			long tagEndIdx = tagStartIdx +  bitsPerTag;
			for (long i = tagStartIdx ; i < tagEndIdx ; i++) {
				if(memBlock.getAndClear(i- bitsPerTag))
					memBlock.set(i);
			}
		}
		//return tag;
	}

	/**
	 *  Finds the bit offset in the bitset for a tag
	 *
	 * @param bucketIndex  the bucket index
	 * @param posInBucket  position in bucket
	 * @return
	 */
	private long getTagOffset(long bucketIndex, int posInBucket) {
		return (bucketIndex * CountingCuckooFilter.BUCKET_SIZE * bitsPerTag) + (posInBucket * bitsPerTag);
	}



	@Override
	public boolean equals(@Nullable Object object) {
		if (object == this) {
			return true;
		}
		if (object instanceof FilterTable) {
			FilterTable that = (FilterTable) object;
			return this.bitsPerTag == that.bitsPerTag && this.memBlock.equals(that.memBlock)
					&& this.numBuckets == that.numBuckets;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(bitsPerTag, memBlock, numBuckets);
	}

	public FilterTable copy() {
		return new FilterTable(memBlock.clone(), bitsPerTag, numBuckets);
	}


	public void printMemBlock(){
		System.out.println("----------------");
		for(long i = 0; i < numBuckets; i++){
			for (int j = 0; j < CountingCuckooFilter.BUCKET_SIZE; j++) { //BUCKET_SIZE = 4
				System.out.print(Long.toBinaryString(readTag(i,j))+",");
			}
			System.out.println();
		}
		System.out.println("-----------------------------");
	}
}
