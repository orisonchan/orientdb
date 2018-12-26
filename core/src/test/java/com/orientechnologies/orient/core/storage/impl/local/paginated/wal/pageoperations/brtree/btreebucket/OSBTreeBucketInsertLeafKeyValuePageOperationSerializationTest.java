package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.brtree.btreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketInsertLeafKeyValuePageOperation;
import org.junit.Assert;
import org.junit.Test;

public class OSBTreeBucketInsertLeafKeyValuePageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 235;
    final int pageIndex = 34;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 456;

    final int keySize = 23;
    final int valueSize = 12;

    OSBTreeBucketInsertLeafKeyValuePageOperation operation = new OSBTreeBucketInsertLeafKeyValuePageOperation(index, keySize,
        valueSize);
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
    Assert.assertEquals(keySize, restoredOperation.getKeySize());
    Assert.assertEquals(valueSize, restoredOperation.getValueSize());
    Assert.assertEquals(index, restoredOperation.getIndex());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 235;
    final int pageIndex = 34;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 456;

    final int keySize = 23;
    final int valueSize = 12;

    OSBTreeBucketInsertLeafKeyValuePageOperation operation = new OSBTreeBucketInsertLeafKeyValuePageOperation(index, keySize,
        valueSize);
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
    Assert.assertEquals(keySize, restoredOperation.getKeySize());
    Assert.assertEquals(valueSize, restoredOperation.getValueSize());
    Assert.assertEquals(index, restoredOperation.getIndex());
  }
}
