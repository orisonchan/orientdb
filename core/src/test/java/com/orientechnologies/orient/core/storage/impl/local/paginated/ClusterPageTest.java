package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OCacheEntryChanges;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 20.03.13
 */
public class ClusterPageTest {
  private static final int SYSTEM_OFFSET = 24;

  @Test
  public void testAddOneRecord() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = null;
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      addOneRecord(localPage);

      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);

      OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);
      addOneRecord(revertedPage);
    } finally {
      cachePointer.decrementReferrer();
      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void addOneRecord(OClusterPage localPage) {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 1;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordsCount(), 1);
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(position, 0);
    Assert.assertFalse(localPage.isDeleted(0));
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    assertThat(localPage.getRecordBinaryValue(0, 0, 11)).isEqualTo(new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
  }

  @Test
  public void testAddThreeRecords() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = null;
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      addThreeRecords(localPage);

      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);

      OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);

      addThreeRecords(revertedPage);
    } finally {
      cachePointer.decrementReferrer();
      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void addThreeRecords(OClusterPage localPage) {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int positionOne = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int positionTwo = localPage.appendRecord(recordVersion, new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    int positionThree = localPage.appendRecord(recordVersion, new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });

    Assert.assertEquals(localPage.getRecordsCount(), 3);
    Assert.assertEquals(positionOne, 0);
    Assert.assertEquals(positionTwo, 1);
    Assert.assertEquals(positionThree, 2);

    Assert.assertFalse(localPage.isDeleted(0));
    Assert.assertFalse(localPage.isDeleted(1));
    Assert.assertFalse(localPage.isDeleted(2));

    assertThat(localPage.getRecordBinaryValue(0, 0, 11)).isEqualTo(new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    assertThat(localPage.getRecordBinaryValue(1, 0, 11)).isEqualTo(new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(localPage.getRecordVersion(1), recordVersion);

    assertThat(localPage.getRecordBinaryValue(2, 0, 11)).isEqualTo(new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(localPage.getRecordVersion(2), recordVersion);
  }

  @Test
  public void testAddFullPage() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = null;
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      addFullPage(localPage);
      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);

      OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);
      addFullPage(revertedPage);
    } finally {
      cachePointer.decrementReferrer();
      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void addFullPage(OClusterPage localPage) {
    int recordVersion = 0;
    recordVersion++;

    List<Integer> positions = new ArrayList<>();
    int lastPosition;
    byte counter = 0;
    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positions.size());
        positions.add(lastPosition);
        counter++;
      }
    } while (lastPosition >= 0);

    Assert.assertEquals(localPage.getRecordsCount(), positions.size());

    counter = 0;
    for (int position : positions) {
      assertThat(localPage.getRecordBinaryValue(position, 0, 3)).isEqualTo(new byte[] { counter, counter, counter });
      Assert.assertEquals(localPage.getRecordSize(position), 3);
      Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
      counter++;
    }
  }

  @Test
  public void testDeleteAddLowerVersion() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = null;
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      deleteAddLowerVersion(localPage);

      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);

      OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);
      deleteAddLowerVersion(revertedPage);
    } finally {
      cachePointer.decrementReferrer();
      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void deleteAddLowerVersion(OClusterPage localPage) {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    Assert.assertTrue(localPage.deleteRecord(position));

    int newRecordVersion = 0;

    Assert.assertEquals(localPage.appendRecord(newRecordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), newRecordVersion);

    assertThat(localPage.getRecordBinaryValue(position, 0, recordSize)).isEqualTo(new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
  }

  @Test
  public void testDeleteAddBiggerVersion() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = null;
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      deleteAddBiggerVersion(localPage);

      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);

      OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);
      deleteAddBiggerVersion(revertedPage);
    } finally {
      cachePointer.decrementReferrer();
      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void deleteAddBiggerVersion(OClusterPage localPage) {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    Assert.assertTrue(localPage.deleteRecord(position));

    int newRecordVersion = 0;
    newRecordVersion++;
    newRecordVersion++;
    newRecordVersion++;
    newRecordVersion++;

    Assert.assertEquals(localPage.appendRecord(newRecordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), newRecordVersion);

    assertThat(localPage.getRecordBinaryValue(position, 0, recordSize)).isEqualTo(new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
  }

  @Test
  public void testDeleteAddEqualVersion() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = null;
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);

    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      deleteAddEqualVersion(localPage);

      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);

      OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);
      deleteAddBiggerVersion(revertedPage);
    } finally {
      cachePointer.decrementReferrer();
      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void deleteAddEqualVersion(OClusterPage localPage) {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    Assert.assertTrue(localPage.deleteRecord(position));

    Assert.assertEquals(localPage.appendRecord(recordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
    assertThat(localPage.getRecordBinaryValue(position, 0, recordSize)).isEqualTo(new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
  }

  @Test
  public void testDeleteAddEqualVersionKeepTombstoneVersion() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = null;
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      deleteAddEqualVersionKeepTombstoneVersion(localPage);

      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);
      OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);

      deleteAddEqualVersionKeepTombstoneVersion(revertedPage);
    } finally {
      cachePointer.decrementReferrer();
      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void deleteAddEqualVersionKeepTombstoneVersion(OClusterPage localPage) {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    Assert.assertTrue(localPage.deleteRecord(position));

    Assert.assertEquals(localPage.appendRecord(recordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
    assertThat(localPage.getRecordBinaryValue(position, 0, recordSize)).isEqualTo(new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
  }

  @Test
  public void testDeleteTwoOutOfFour() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = null;
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      deleteTwoOutOfFour(localPage);

      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);
      OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);

      deleteTwoOutOfFour(revertedPage);
    } finally {
      cachePointer.decrementReferrer();
      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void deleteTwoOutOfFour(OClusterPage localPage) {
    int recordVersion = 0;
    recordVersion++;

    int positionOne = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int positionTwo = localPage.appendRecord(recordVersion, new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    int positionThree = localPage.appendRecord(recordVersion, new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });
    int positionFour = localPage.appendRecord(recordVersion, new byte[] { 4, 2, 3, 4, 5, 6, 5, 4, 3, 2, 4 });

    Assert.assertEquals(localPage.getRecordsCount(), 4);
    Assert.assertEquals(positionOne, 0);
    Assert.assertEquals(positionTwo, 1);
    Assert.assertEquals(positionThree, 2);
    Assert.assertEquals(positionFour, 3);

    Assert.assertFalse(localPage.isDeleted(0));
    Assert.assertFalse(localPage.isDeleted(1));
    Assert.assertFalse(localPage.isDeleted(2));
    Assert.assertFalse(localPage.isDeleted(3));

    int freeSpace = localPage.getFreeSpace();

    Assert.assertTrue(localPage.deleteRecord(0));
    Assert.assertTrue(localPage.deleteRecord(2));

    Assert.assertFalse(localPage.deleteRecord(0));
    Assert.assertFalse(localPage.deleteRecord(7));

    Assert.assertEquals(localPage.findFirstDeletedRecord(0), 0);
    Assert.assertEquals(localPage.findFirstDeletedRecord(1), 2);
    Assert.assertEquals(localPage.findFirstDeletedRecord(3), -1);

    Assert.assertTrue(localPage.isDeleted(0));
    Assert.assertEquals(localPage.getRecordSize(0), -1);
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    assertThat(localPage.getRecordBinaryValue(1, 0, 11)).isEqualTo(new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    Assert.assertEquals(localPage.getRecordSize(1), 11);
    Assert.assertEquals(localPage.getRecordVersion(1), recordVersion);

    Assert.assertTrue(localPage.isDeleted(2));
    Assert.assertEquals(localPage.getRecordSize(2), -1);
    Assert.assertEquals(localPage.getRecordVersion(2), recordVersion);

    assertThat(localPage.getRecordBinaryValue(3, 0, 11)).isEqualTo(new byte[] { 4, 2, 3, 4, 5, 6, 5, 4, 3, 2, 4 });

    Assert.assertEquals(localPage.getRecordSize(3), 11);
    Assert.assertEquals(localPage.getRecordVersion(3), recordVersion);

    Assert.assertEquals(localPage.getRecordsCount(), 2);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace + 23 * 2);
  }

  @Test
  public void testAddFullPageDeleteAndAddAgain() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = null;
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      addFullPageDeleteAndAddAgain(localPage);

      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);
      OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);

      addFullPageDeleteAndAddAgain(revertedPage);
    } finally {
      cachePointer.decrementReferrer();
      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void addFullPageDeleteAndAddAgain(OClusterPage localPage) {
    Map<Integer, Byte> positionCounter = new HashMap<>();
    Set<Integer> deletedPositions = new HashSet<>();

    int lastPosition;
    byte counter = 0;
    int recordVersion = 0;
    recordVersion++;

    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positionCounter.size());
        positionCounter.put(lastPosition, counter);
        counter++;

      }
    } while (lastPosition >= 0);

    int filledRecordsCount = positionCounter.size();
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);

    for (int i = 0; i < filledRecordsCount; i += 2) {
      localPage.deleteRecord(i);
      deletedPositions.add(i);
      positionCounter.remove(i);
    }

    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        positionCounter.put(lastPosition, counter);
        counter++;
      }
    } while (lastPosition >= 0);

    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);
    for (Map.Entry<Integer, Byte> entry : positionCounter.entrySet()) {
      assertThat(localPage.getRecordBinaryValue(entry.getKey(), 0, 3))
          .isEqualTo(new byte[] { entry.getValue(), entry.getValue(), entry.getValue() });

      Assert.assertEquals(localPage.getRecordSize(entry.getKey()), 3);

      if (deletedPositions.contains(entry.getKey()))
        Assert.assertEquals(localPage.getRecordVersion(entry.getKey()), recordVersion);

    }
  }

  @Test
  public void testAddBigRecordDeleteAndAddSmallRecords() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = null;
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    try {
      final long seed = System.currentTimeMillis();

      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      addBigRecordDeleteAndAddSmallRecords(seed, localPage);

      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);
      OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);

      addBigRecordDeleteAndAddSmallRecords(seed, revertedPage);
    } finally {
      cachePointer.decrementReferrer();
      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void addBigRecordDeleteAndAddSmallRecords(long seed, OClusterPage localPage) {
    final Random mersenneTwisterFast = new Random(seed);

    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    final byte[] bigChunk = new byte[OClusterPage.MAX_ENTRY_SIZE / 2];

    mersenneTwisterFast.nextBytes(bigChunk);

    int position = localPage.appendRecord(recordVersion, bigChunk);
    Assert.assertEquals(position, 0);
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    Assert.assertTrue(localPage.deleteRecord(0));

    recordVersion++;
    Map<Integer, Byte> positionCounter = new HashMap<>();
    int lastPosition;
    byte counter = 0;
    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positionCounter.size());
        positionCounter.put(lastPosition, counter);
        counter++;
      }
    } while (lastPosition >= 0);

    Assert.assertEquals(localPage.getRecordsCount(), positionCounter.size());
    for (Map.Entry<Integer, Byte> entry : positionCounter.entrySet()) {
      assertThat(localPage.getRecordBinaryValue(entry.getKey(), 0, 3))
          .isEqualTo(new byte[] { entry.getValue(), entry.getValue(), entry.getValue() });
      Assert.assertEquals(localPage.getRecordSize(entry.getKey()), 3);
      Assert.assertEquals(localPage.getRecordVersion(entry.getKey()), recordVersion);
    }
  }

  @Test
  public void testFindFirstRecord() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);

    OCacheEntry revertedCacheEntry = null;
    final long seed = System.currentTimeMillis();
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      findFirstRecord(seed, localPage);

      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);

      OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);
      findFirstRecord(seed, revertedPage);
    } finally {
      cachePointer.decrementReferrer();
      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void findFirstRecord(long seed, OClusterPage localPage) {
    final Random mersenneTwister = new Random(seed);
    Set<Integer> positions = new HashSet<>();

    int lastPosition;
    byte counter = 0;

    int recordVersion = 0;
    recordVersion++;

    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positions.size());
        positions.add(lastPosition);
        counter++;
      }
    } while (lastPosition >= 0);

    int filledRecordsCount = positions.size();
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);

    for (int i = 0; i < filledRecordsCount; i++) {
      if (mersenneTwister.nextBoolean()) {
        localPage.deleteRecord(i);
        positions.remove(i);
      }
    }

    int recordsIterated = 0;
    int recordPosition = 0;
    int lastRecordPosition = -1;

    do {
      recordPosition = localPage.findFirstRecord(recordPosition);
      if (recordPosition < 0)
        break;

      Assert.assertTrue(positions.contains(recordPosition));
      Assert.assertTrue(recordPosition > lastRecordPosition);

      lastRecordPosition = recordPosition;

      recordPosition++;
      recordsIterated++;
    } while (recordPosition >= 0);

    Assert.assertEquals(recordsIterated, positions.size());
  }

  @Test
  public void testFindLastRecord() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = null;
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    final long seed = System.currentTimeMillis();
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      findLastRecord(seed, localPage);

      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);
      OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);

      findFirstRecord(seed, revertedPage);
    } finally {
      cachePointer.decrementReferrer();
      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void findLastRecord(long seed, OClusterPage localPage) {
    final Random mersenneTwister = new Random(seed);
    Set<Integer> positions = new HashSet<>();

    int lastPosition;
    byte counter = 0;

    int recordVersion = 0;
    recordVersion++;

    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positions.size());
        positions.add(lastPosition);
        counter++;
      }
    } while (lastPosition >= 0);

    int filledRecordsCount = positions.size();
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);

    for (int i = 0; i < filledRecordsCount; i++) {
      if (mersenneTwister.nextBoolean()) {
        localPage.deleteRecord(i);
        positions.remove(i);
      }
    }

    int recordsIterated = 0;
    int recordPosition = Integer.MAX_VALUE;
    int lastRecordPosition = Integer.MAX_VALUE;
    do {
      recordPosition = localPage.findLastRecord(recordPosition);
      if (recordPosition < 0)
        break;

      Assert.assertTrue(positions.contains(recordPosition));
      Assert.assertTrue(recordPosition < lastRecordPosition);

      recordPosition--;
      recordsIterated++;
    } while (recordPosition >= 0);

    Assert.assertEquals(recordsIterated, positions.size());
  }

  @Test
  public void testSetGetNextPage() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = null;
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      setGetNextPage(localPage);

      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);
    } finally {
      cachePointer.decrementReferrer();

      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void setGetNextPage(OClusterPage localPage) {
    localPage.setNextPage(1034);
    Assert.assertEquals(localPage.getNextPage(), 1034);
  }

  @Test
  public void testSetGetPrevPage() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = null;
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      setGetPrevPage(localPage);

      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);

      OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);
      setGetPrevPage(revertedPage);
    } finally {
      cachePointer.decrementReferrer();
      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void setGetPrevPage(OClusterPage localPage) {
    localPage.setPrevPage(1034);
    Assert.assertEquals(localPage.getPrevPage(), 1034);
  }

  @Test
  public void testReplaceOneRecordWithEqualSize() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = null;
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      replaceOneRecordWithEqualSize(localPage);

      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);
      OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);

      replaceOneRecordWithEqualSize(revertedPage);
    } finally {
      cachePointer.decrementReferrer();

      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void replaceOneRecordWithEqualSize(OClusterPage localPage) {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion;
    newRecordVersion = recordVersion;
    newRecordVersion++;

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 }, newRecordVersion);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 11);

    Assert.assertEquals(localPage.getRecordSize(index), 11);

    assertThat(localPage.getRecordBinaryValue(index, 0, 11)).isEqualTo(new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordVersion(index), newRecordVersion);
  }

  @Test
  public void testReplaceOneRecordNoVersionUpdate() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = null;
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      replaceOneRecordNoVersionUpdate(localPage);

      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);

      OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);
      replaceOneRecordNoVersionUpdate(revertedPage);
    } finally {
      cachePointer.decrementReferrer();

      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void replaceOneRecordNoVersionUpdate(OClusterPage localPage) {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 9 });
    int freeSpace = localPage.getFreeSpace();

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1, 3 }, -1);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 12);

    Assert.assertEquals(localPage.getRecordSize(index), 12);

    assertThat(localPage.getRecordBinaryValue(index, 0, 12)).isEqualTo(new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1, 3 });
    Assert.assertEquals(localPage.getRecordVersion(index), recordVersion);
  }

  @Test
  public void testReplaceOneRecordLowerVersion() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = null;
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);

      replaceOneRecordLowerVersion(localPage);

      assertChangesTracking(localPage, bufferPool, pointer);

      revertedCacheEntry = revertChanges(localPage, bufferPool);
      OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);
      replaceOneRecordLowerVersion(revertedPage);
    } finally {
      cachePointer.decrementReferrer();

      if (revertedCacheEntry != null) {
        revertedCacheEntry.getCachePointer().decrementReferrer();
      }
    }
  }

  private void replaceOneRecordLowerVersion(OClusterPage localPage) {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion;
    newRecordVersion = recordVersion;

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 }, newRecordVersion);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 11);

    Assert.assertEquals(localPage.getRecordSize(index), 11);

    assertThat(localPage.getRecordBinaryValue(index, 0, 11)).isEqualTo(new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordVersion(index), recordVersion);
  }

  private void assertChangesTracking(OClusterPage localPage, OByteBufferPool bufferPool, OPointer pointer) throws IOException {
    OPointer restoredPointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(restoredPointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);

    OWriteCache writeCache = Mockito.mock(OWriteCache.class);
    OReadCache readCache = Mockito.mock(OReadCache.class);

    when(readCache
        .loadForWrite(anyLong(), anyLong(), anyBoolean(), anyObject(), anyInt(), anyBoolean(), (OLogSequenceNumber) isNull()))
        .thenReturn(cacheEntry);
    try {
      final List<OPageOperationRecord> operations = localPage.getOperations();

      for (OPageOperationRecord operation : operations) {
        operation.redo(readCache, writeCache);
      }

      assertThat(getBytes(restoredPointer.getNativeByteBuffer(), OClusterPage.PAGE_SIZE - SYSTEM_OFFSET))
          .isEqualTo(getBytes(pointer.getNativeByteBuffer(), OClusterPage.PAGE_SIZE - SYSTEM_OFFSET));
    } finally {
      cachePointer.decrementReferrer();
    }
  }

  private OCacheEntry revertChanges(OClusterPage localPage, OByteBufferPool bufferPool) throws IOException {
    OPointer revertedPointer = bufferPool.acquireDirect(true);

    OCachePointer revertedCachePointer = new OCachePointer(revertedPointer, bufferPool, 0, 0);
    revertedCachePointer.incrementReferrer();

    OCacheEntry revertedCacheEntry = new OCacheEntryImpl(0, 0, revertedCachePointer);

    ByteBuffer revertedBuffer = revertedCacheEntry.getCachePointer().getBufferDuplicate();

    OCacheEntry cacheEntry = localPage.getCacheEntry();
    ByteBuffer buffer = cacheEntry.getCachePointer().getBufferDuplicate();

    assert buffer != null;
    assert revertedBuffer != null;

    buffer.position(0);
    revertedBuffer.position(0);

    revertedBuffer.put(buffer);
    revertedBuffer.position(0);

    OWriteCache writeCache = Mockito.mock(OWriteCache.class);
    OReadCache readCache = Mockito.mock(OReadCache.class);

    when(readCache
        .loadForWrite(anyLong(), anyLong(), anyBoolean(), anyObject(), anyInt(), anyBoolean(), (OLogSequenceNumber) isNull()))
        .thenReturn(revertedCacheEntry);

    final List<OPageOperationRecord> operations = new ArrayList<>(localPage.getOperations());
    Collections.reverse(operations);

    for (OPageOperationRecord operation : operations) {
      operation.undo(readCache, writeCache);
    }

    OClusterPage revertedPage = new OClusterPage(revertedCacheEntry, false);
    Assert.assertTrue(revertedPage.isEmpty());

    return revertedCacheEntry;
  }

  private byte[] getBytes(ByteBuffer buffer, int len) {
    byte[] result = new byte[len];
    buffer.position(ClusterPageTest.SYSTEM_OFFSET);
    buffer.get(result);

    return result;
  }
}
