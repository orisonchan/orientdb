package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OHashIndexBucketUpdateEntryPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final long fileId = 456;
    final int pageIndex = 45;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 345;
    final int keySize = 54;
    final int valueSize = 65;
    final byte[] oldValue = new byte[45];

    final Random random = new Random();
    random.nextBytes(oldValue);

    OHashIndexBucketUpdateEntryPageOperation operation = new OHashIndexBucketUpdateEntryPageOperation(index, keySize, valueSize,
        oldValue);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OHashIndexBucketUpdateEntryPageOperation restoredOperation = new OHashIndexBucketUpdateEntryPageOperation();

    offset = restoredOperation.fromStream(stream, 1);
    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(keySize, restoredOperation.getKeySize());
    Assert.assertEquals(valueSize, restoredOperation.getValueSize());

    Assert.assertArrayEquals(oldValue, restoredOperation.getOldValue());
  }

  @Test
  public void testBufferSerialization() {
    final long fileId = 456;
    final int pageIndex = 45;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 345;
    final int keySize = 54;
    final int valueSize = 65;
    final byte[] oldValue = new byte[45];

    final Random random = new Random();
    random.nextBytes(oldValue);

    OHashIndexBucketUpdateEntryPageOperation operation = new OHashIndexBucketUpdateEntryPageOperation(index, keySize, valueSize,
        oldValue);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OHashIndexBucketUpdateEntryPageOperation restoredOperation = new OHashIndexBucketUpdateEntryPageOperation();

    int offset = restoredOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(keySize, restoredOperation.getKeySize());
    Assert.assertEquals(valueSize, restoredOperation.getValueSize());

    Assert.assertArrayEquals(oldValue, restoredOperation.getOldValue());
  }

}
