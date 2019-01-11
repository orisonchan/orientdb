package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 4/15/14
 */
public class ONullBucketTest {
  @Test
  public void testEmptyBucket() {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<>(cacheEntry);
    Assert.assertNull(bucket.getValue(OStringSerializer.INSTANCE));

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

  @Test
  public void testAddGetValue() {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<>(cacheEntry);

    bucket.setValue(new OSBTreeValue<>(false, -1, "test"), OStringSerializer.INSTANCE, -1);
    OSBTreeValue<String> treeValue = bucket.getValue(OStringSerializer.INSTANCE);
    assert treeValue != null;
    Assert.assertEquals(treeValue.getValue(), "test");

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

  @Test
  public void testAddRemoveValue() {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<>(cacheEntry);

    bucket.setValue(new OSBTreeValue<>(false, -1, "test"), OStringSerializer.INSTANCE, -1);
    bucket.removeValue(OStringSerializer.INSTANCE.getObjectSize("test"));

    OSBTreeValue<String> treeValue = bucket.getValue(OStringSerializer.INSTANCE);
    Assert.assertNull(treeValue);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

  @Test
  public void testAddRemoveAddValue() {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<>(cacheEntry);

    bucket.setValue(new OSBTreeValue<>(false, -1, "test"), OStringSerializer.INSTANCE, -1);
    bucket.removeValue(OStringSerializer.INSTANCE.getObjectSize("test"));

    OSBTreeValue<String> treeValue = bucket.getValue(OStringSerializer.INSTANCE);
    Assert.assertNull(treeValue);

    bucket.setValue(new OSBTreeValue<>(false, -1, "testOne"), OStringSerializer.INSTANCE, -1);

    treeValue = bucket.getValue(OStringSerializer.INSTANCE);
    Assert.assertNotNull(treeValue);
    Assert.assertEquals(treeValue.getValue(), "testOne");

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

}
