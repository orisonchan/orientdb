package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.brtree.btreenullbucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreenullbucket.OSBTreeNullBucketRemoveValuePageOperation;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OSBTreeNullBucketRemoveValuePageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 234;
    final int pageIndex = 432;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final byte[] prevValue = new byte[12];
    final Random random = new Random();
    random.nextBytes(prevValue);

    OSBTreeNullBucketRemoveValuePageOperation operation = new OSBTreeNullBucketRemoveValuePageOperation(prevValue);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);
    Assert.assertEquals(serializedSize + 1, offset);

    OSBTreeNullBucketRemoveValuePageOperation restoredOperation = new OSBTreeNullBucketRemoveValuePageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertArrayEquals(prevValue, restoredOperation.getPreviousValue());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 234;
    final int pageIndex = 432;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final byte[] prevValue = new byte[12];
    final Random random = new Random();
    random.nextBytes(prevValue);

    OSBTreeNullBucketRemoveValuePageOperation operation = new OSBTreeNullBucketRemoveValuePageOperation(prevValue);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeNullBucketRemoveValuePageOperation restoredOperation = new OSBTreeNullBucketRemoveValuePageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertArrayEquals(prevValue, restoredOperation.getPreviousValue());
  }
}
