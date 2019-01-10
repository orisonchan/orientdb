package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.nullbucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OHashIndexNullBucketRemoveValuePageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final long fileId = 456;
    final int pageIndex = 45;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final byte[] oldValue = new byte[12];

    final Random random = new Random();
    random.nextBytes(oldValue);

    OHashIndexNullBucketRemoveValuePageOperation operation = new OHashIndexNullBucketRemoveValuePageOperation(oldValue);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OHashIndexNullBucketRemoveValuePageOperation restoredOperation = new OHashIndexNullBucketRemoveValuePageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertArrayEquals(oldValue, restoredOperation.getOldValue());
  }

  @Test
  public void testBufferSerialization() {
    final long fileId = 456;
    final int pageIndex = 45;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final byte[] oldValue = new byte[12];

    final Random random = new Random();
    random.nextBytes(oldValue);

    OHashIndexNullBucketRemoveValuePageOperation operation = new OHashIndexNullBucketRemoveValuePageOperation(oldValue);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OHashIndexNullBucketRemoveValuePageOperation restoredOperation = new OHashIndexNullBucketRemoveValuePageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertArrayEquals(oldValue, restoredOperation.getOldValue());
  }

}
