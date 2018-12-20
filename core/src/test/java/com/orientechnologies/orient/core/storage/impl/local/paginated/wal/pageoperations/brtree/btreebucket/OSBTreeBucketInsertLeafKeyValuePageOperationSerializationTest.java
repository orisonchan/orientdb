package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.brtree.btreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketInsertLeafKeyValuePageOperation;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class OSBTreeBucketInsertLeafKeyValuePageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 235;
    final int pageIndex = 34;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final Random random = new Random();
    final int index = 456;

    final byte[] serializedKey = new byte[23];
    random.nextBytes(serializedKey);

    final byte[] serializedValue = new byte[12];
    random.nextBytes(serializedValue);

    OSBTreeBucketInsertLeafKeyValuePageOperation operation = new OSBTreeBucketInsertLeafKeyValuePageOperation(index, serializedKey,
        serializedValue);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OSBTreeBucketInsertLeafKeyValuePageOperation restoredOperation = new OSBTreeBucketInsertLeafKeyValuePageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertArrayEquals(serializedKey, restoredOperation.getSerializedKey());
    Assert.assertArrayEquals(serializedValue, restoredOperation.getSerializedValue());
    Assert.assertEquals(index, restoredOperation.getIndex());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 235;
    final int pageIndex = 34;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final Random random = new Random();
    final int index = 456;

    final byte[] serializedKey = new byte[23];
    random.nextBytes(serializedKey);

    final byte[] serializedValue = new byte[12];
    random.nextBytes(serializedValue);

    OSBTreeBucketInsertLeafKeyValuePageOperation operation = new OSBTreeBucketInsertLeafKeyValuePageOperation(index, serializedKey,
        serializedValue);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OSBTreeBucketInsertLeafKeyValuePageOperation restoredOperation = new OSBTreeBucketInsertLeafKeyValuePageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertArrayEquals(serializedKey, restoredOperation.getSerializedKey());
    Assert.assertArrayEquals(serializedValue, restoredOperation.getSerializedValue());
    Assert.assertEquals(index, restoredOperation.getIndex());
  }
}
