package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OHashIndexBucketSetDepthPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int pageIndex = 34;
    final long fileId = 543;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final byte oldDepth = 45;

    final OHashIndexBucketSetDepthPageOperation operation = new OHashIndexBucketSetDepthPageOperation(oldDepth);
    operation.setPageIndex(34);
    operation.setFileId(fileId);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    OHashIndexBucketSetDepthPageOperation restoredOperation = new OHashIndexBucketSetDepthPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(oldDepth, restoredOperation.getOldDepth());
  }

  @Test
  public void testBufferSerialization() {
    final int pageIndex = 34;
    final long fileId = 543;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final byte oldDepth = 45;

    final OHashIndexBucketSetDepthPageOperation operation = new OHashIndexBucketSetDepthPageOperation(oldDepth);
    operation.setPageIndex(34);
    operation.setFileId(fileId);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());
    OHashIndexBucketSetDepthPageOperation restoredOperation = new OHashIndexBucketSetDepthPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(oldDepth, restoredOperation.getOldDepth());
  }
}
