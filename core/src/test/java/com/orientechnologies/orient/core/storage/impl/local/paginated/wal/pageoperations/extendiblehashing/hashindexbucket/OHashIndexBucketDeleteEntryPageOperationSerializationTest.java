package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OHashIndexBucketDeleteEntryPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final long fileId = 456;
    final int pageIndex = 34;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 43;
    final long hashCode = 345;

    final byte[] key = new byte[24];
    final byte[] value = new byte[12];

    final Random random = new Random();
    random.nextBytes(key);
    random.nextBytes(value);

    OHashIndexBucketDeleteEntryPageOperation operation = new OHashIndexBucketDeleteEntryPageOperation(index, hashCode, key, value);
    operation.setOperationUnitId(operationUnitId);
    operation.setPageIndex(pageIndex);
    operation.setFileId(fileId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];

    int offset = operation.toStream(stream, 1);
    Assert.assertEquals(serializedSize + 1, offset);

    OHashIndexBucketDeleteEntryPageOperation restoredOperation = new OHashIndexBucketDeleteEntryPageOperation();
    offset = restoredOperation.fromStream(stream, 1);
    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(hashCode, restoredOperation.getHashCode());
    Assert.assertArrayEquals(key, restoredOperation.getKey());
    Assert.assertArrayEquals(value, restoredOperation.getValue());
  }

  @Test
  public void testBufferSerialization() {
    final long fileId = 456;
    final int pageIndex = 34;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 43;
    final long hashCode = 345;

    final byte[] key = new byte[24];
    final byte[] value = new byte[12];

    final Random random = new Random();
    random.nextBytes(key);
    random.nextBytes(value);

    OHashIndexBucketDeleteEntryPageOperation operation = new OHashIndexBucketDeleteEntryPageOperation(index, hashCode, key, value);
    operation.setOperationUnitId(operationUnitId);
    operation.setPageIndex(pageIndex);
    operation.setFileId(fileId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OHashIndexBucketDeleteEntryPageOperation restoredOperation = new OHashIndexBucketDeleteEntryPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(hashCode, restoredOperation.getHashCode());
    Assert.assertArrayEquals(key, restoredOperation.getKey());
    Assert.assertArrayEquals(value, restoredOperation.getValue());
  }
}
