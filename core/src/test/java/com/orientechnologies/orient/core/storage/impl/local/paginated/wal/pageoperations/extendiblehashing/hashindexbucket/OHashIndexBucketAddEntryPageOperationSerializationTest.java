package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OHashIndexBucketAddEntryPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int pageIndex = 12;
    final long fileId = 567;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int index = 54;
    final int keySize = 5;
    final int valueSize = 43;

    OHashIndexBucketAddEntryPageOperation operation = new OHashIndexBucketAddEntryPageOperation(index, keySize, valueSize);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);
    Assert.assertEquals(serializedSize + 1, offset);

    OHashIndexBucketAddEntryPageOperation restoredOperation = new OHashIndexBucketAddEntryPageOperation();
    offset = restoredOperation.fromStream(stream, 1);
    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(keySize, restoredOperation.getKeySize());
    Assert.assertEquals(valueSize, restoredOperation.getValueSize());
  }

  @Test
  public void testBufferSerialization() {
    final int pageIndex = 12;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int index = 54;
    final int keySize = 5;
    final int valueSize = 43;
    final long fileId = 3345;

    OHashIndexBucketAddEntryPageOperation operation = new OHashIndexBucketAddEntryPageOperation(index, keySize, valueSize);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OHashIndexBucketAddEntryPageOperation restoredOperation = new OHashIndexBucketAddEntryPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(keySize, restoredOperation.getKeySize());
    Assert.assertEquals(valueSize, restoredOperation.getValueSize());
  }

}
