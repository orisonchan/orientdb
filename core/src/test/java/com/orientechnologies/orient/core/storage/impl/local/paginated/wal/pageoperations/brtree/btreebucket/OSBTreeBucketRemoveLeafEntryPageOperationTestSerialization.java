package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.brtree.btreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketRemoveLeafEntryPageOperation;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OSBTreeBucketRemoveLeafEntryPageOperationTestSerialization {
  @Test
  public void testStreamSerialization() {
    final int fileId = 342;
    final int pageIndex = 45;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int entryIndex = 34;
    final Random random = new Random();

    final byte[] key = new byte[12];
    random.nextBytes(key);

    final byte[] value = new byte[23];
    random.nextBytes(value);

    OSBTreeBucketRemoveLeafEntryPageOperation operation = new OSBTreeBucketRemoveLeafEntryPageOperation(entryIndex, key, value);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OSBTreeBucketRemoveLeafEntryPageOperation restoredOperation = new OSBTreeBucketRemoveLeafEntryPageOperation();
    offset = restoredOperation.fromStream(stream, 1);
    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertArrayEquals(key, restoredOperation.getRawKey());
    Assert.assertArrayEquals(value, restoredOperation.getRawValue());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 342;
    final int pageIndex = 45;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int entryIndex = 34;
    final Random random = new Random();

    final byte[] key = new byte[12];
    random.nextBytes(key);

    final byte[] value = new byte[23];
    random.nextBytes(value);

    OSBTreeBucketRemoveLeafEntryPageOperation operation = new OSBTreeBucketRemoveLeafEntryPageOperation(entryIndex, key, value);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBucketRemoveLeafEntryPageOperation restoredOperation = new OSBTreeBucketRemoveLeafEntryPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertArrayEquals(key, restoredOperation.getRawKey());
    Assert.assertArrayEquals(value, restoredOperation.getRawValue());
  }
}
