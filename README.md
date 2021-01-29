# CountingCuckooFilter4J


This code is based on CuckooFilter4J. (https://github.com/MGunlogson/CuckooFilter4J)

The Cuckoo Filter is a variant of the Bloom filter.

About Cuckoo Filter: https://dl.acm.org/doi/abs/10.1145/2674005.2674994


The original version of the cuckoo filter had a problem:
>assuming the size of each bucket was _k_, the same element (or hash collision) could be inserted up to _2*k_ times.

We introduce a simple data structure. 
When an element/fingerprint is inserted more than 3 times, we use a structure that takes up 3 consecutive spaces instead:

|element|0|count|
|:-----:|:----:| :----: |

---
When inserting the same element: `count++`

>The length of count is determined by the element/fingerprint size in the bucket.
>>Assuming that each element occupies b bits, the count is repeated as 3+(2^b)-1

---
When deleting an element: `count--`

If `count == 0`, use 3 same elements to occupy this part of the space.

Additional costs are:
1. Each insert needs to check the number of inserts in 2 buckets.
2. After deleting an element, the code needs to shift all the following elements in the bucket to the current position.

**The good news is that the lookup time is the same as the original version.**