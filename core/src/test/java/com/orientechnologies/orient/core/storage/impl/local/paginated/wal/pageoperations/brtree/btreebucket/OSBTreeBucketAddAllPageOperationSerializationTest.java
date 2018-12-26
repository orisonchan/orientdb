package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.brtree.btreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketAddAllPageOperation;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OSBTreeBucketAddAllPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final long fileId = 123;
    final int pageIndex = 12;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int size = 5;

    final byte keySerializerId = 2;
    final byte valueSerializerId = 5;
    final boolean isEncrypted = true;

    OSBTreeBucketAddAllPageOperation operation = new OSBTreeBucketAddAllPageOperation(keySerializerId, valueSerializerId,
        isEncrypted, size);
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
    Assert.assertEquals(keySerializerId, restoredOperation.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredOperation.getValueSerializerId());
    Assert.assertEquals(isEncrypted, restoredOperation.isEncrypted());
    Assert.assertEquals(size, restoredOperation.getEntriesCount());
  }

  @Test
  public void testBufferSerialization() {
    final long fileId = 123;
    final int pageIndex = 12;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int size = 5;

    final byte keySerializerId = 2;
    final byte valueSerializerId = 5;
    final boolean isEncrypted = true;

    OSBTreeBucketAddAllPageOperation operation = new OSBTreeBucketAddAllPageOperation(keySerializerId, valueSerializerId,
        isEncrypted, size);
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
    Assert.assertEquals(keySerializerId, restoredOperation.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredOperation.getValueSerializerId());
    Assert.assertEquals(isEncrypted, restoredOperation.isEncrypted());
    Assert.assertEquals(size, restoredOperation.getEntriesCount());
  }

}
