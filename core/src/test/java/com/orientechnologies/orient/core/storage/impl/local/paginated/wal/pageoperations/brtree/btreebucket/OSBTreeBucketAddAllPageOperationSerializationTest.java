package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.brtree.btreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketAddAllPageOperation;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OSBTreeBucketAddAllPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final long fileId = 123;
    final int pageIndex = 12;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final List<byte[]> entries = new ArrayList<>();

    long seed = System.nanoTime();
    System.out.println("testStreamSerialization seed: " + seed);

    final Random random = new Random();
    final int size = random.nextInt(5);

    for (int i = 0; i < size; i++) {
      final int entrySize = random.nextInt(50) + 1;
      final byte[] entry = new byte[entrySize];
      random.nextBytes(entry);

      entries.add(entry);
    }

    OSBTreeBucketAddAllPageOperation operation = new OSBTreeBucketAddAllPageOperation(entries);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];

    int offset = operation.toStream(stream, 1);
    Assert.assertEquals(serializedSize + 1, offset);

    OSBTreeBucketAddAllPageOperation restoredOperation = new OSBTreeBucketAddAllPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    final List<byte[]> restoredEntries = restoredOperation.getEntries();
    Assert.assertEquals(entries.size(), restoredEntries.size());

    for (int i = 0; i < entries.size(); i++) {
      final byte[] entry = entries.get(i);
      final byte[] restoredEntry = restoredEntries.get(i);

      Assert.assertArrayEquals(entry, restoredEntry);
    }
  }

  @Test
  public void testBufferSerialization() {
    final long fileId = 123;
    final int pageIndex = 12;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final List<byte[]> entries = new ArrayList<>();

    long seed = System.nanoTime();
    System.out.println("testBufferSerialization seed: " + seed);

    final Random random = new Random();
    final int size = random.nextInt(5);

    for (int i = 0; i < size; i++) {
      final int entrySize = random.nextInt(50) + 1;
      final byte[] entry = new byte[entrySize];
      random.nextBytes(entry);

      entries.add(entry);
    }

    OSBTreeBucketAddAllPageOperation operation = new OSBTreeBucketAddAllPageOperation(entries);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBucketAddAllPageOperation restoredOperation = new OSBTreeBucketAddAllPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    final List<byte[]> restoredEntries = restoredOperation.getEntries();
    Assert.assertEquals(entries.size(), restoredEntries.size());

    for (int i = 0; i < entries.size(); i++) {
      final byte[] entry = entries.get(i);
      final byte[] restoredEntry = restoredEntries.get(i);

      Assert.assertArrayEquals(entry, restoredEntry);
    }
  }

}
