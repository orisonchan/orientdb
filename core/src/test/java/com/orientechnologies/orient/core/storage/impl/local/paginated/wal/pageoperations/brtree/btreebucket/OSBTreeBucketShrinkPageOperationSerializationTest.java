package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.brtree.btreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketShrinkPageOperation;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OSBTreeBucketShrinkPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 123;
    final int pageIndex = 321;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final long seed = System.nanoTime();
    final Random random = new Random(seed);

    final boolean isEncrypted = true;

    final List<byte[]> removed = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      final int entrySize = random.nextInt(30);
      final byte[] entry = new byte[entrySize];
      random.nextBytes(entry);
      removed.add(entry);
    }

    final byte keySerializerId = 3;
    final byte valueSerializerId = 5;

    OSBTreeBucketShrinkPageOperation operation = new OSBTreeBucketShrinkPageOperation(removed, keySerializerId,
        valueSerializerId, isEncrypted);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OSBTreeBucketShrinkPageOperation restoredOperation = new OSBTreeBucketShrinkPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(isEncrypted, restoredOperation.isEncrypted());
    Assert.assertEquals(keySerializerId, restoredOperation.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredOperation.getValueSerializerId());

    final List<byte[]> restoredEntries = restoredOperation.getRemovedEntries();
    Assert.assertEquals(removed.size(), restoredEntries.size());
    for (int i = 0; i < removed.size(); i++) {
      final byte[] entry = removed.get(i);
      final byte[] restoredEntry = restoredEntries.get(i);

      Assert.assertArrayEquals(entry, restoredEntry);
    }
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 123;
    final int pageIndex = 321;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final long seed = System.nanoTime();
    final Random random = new Random(seed);

    final boolean isEncrypted = true;

    final List<byte[]> removed = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      final int entrySize = random.nextInt(30);
      final byte[] entry = new byte[entrySize];
      random.nextBytes(entry);
      removed.add(entry);
    }

    final byte keySerializerId = 3;
    final byte valueSerializerId = 5;

    OSBTreeBucketShrinkPageOperation operation = new OSBTreeBucketShrinkPageOperation(removed, keySerializerId,
        valueSerializerId, isEncrypted);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBucketShrinkPageOperation restoredOperation = new OSBTreeBucketShrinkPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(isEncrypted, restoredOperation.isEncrypted());
    Assert.assertEquals(keySerializerId, restoredOperation.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredOperation.getValueSerializerId());

    final List<byte[]> restoredEntries = restoredOperation.getRemovedEntries();
    Assert.assertEquals(removed.size(), restoredEntries.size());
    for (int i = 0; i < removed.size(); i++) {
      final byte[] entry = removed.get(i);
      final byte[] restoredEntry = restoredEntries.get(i);

      Assert.assertArrayEquals(entry, restoredEntry);
    }
  }

}
