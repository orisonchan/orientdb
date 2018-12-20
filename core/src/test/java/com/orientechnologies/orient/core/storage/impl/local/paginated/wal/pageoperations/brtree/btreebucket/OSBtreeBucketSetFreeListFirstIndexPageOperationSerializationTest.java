package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.brtree.btreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBtreeBucketSetFreeListFirstIndexPageOperation;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OSBtreeBucketSetFreeListFirstIndexPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 345;
    final int pageIndex = 34;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int freeListPageIndex = 43;
    final int prevFreeListPageIndex = 56;

    OSBtreeBucketSetFreeListFirstIndexPageOperation operation = new OSBtreeBucketSetFreeListFirstIndexPageOperation(
        freeListPageIndex, prevFreeListPageIndex);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OSBtreeBucketSetFreeListFirstIndexPageOperation restoredOperation = new OSBtreeBucketSetFreeListFirstIndexPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(freeListPageIndex, restoredOperation.getFreeListPageIndex());
    Assert.assertEquals(prevFreeListPageIndex, restoredOperation.getPrevPageIndex());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 345;
    final int pageIndex = 34;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int freeListPageIndex = 43;
    final int prevFreeListPageIndex = 56;

    OSBtreeBucketSetFreeListFirstIndexPageOperation operation = new OSBtreeBucketSetFreeListFirstIndexPageOperation(
        freeListPageIndex, prevFreeListPageIndex);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBtreeBucketSetFreeListFirstIndexPageOperation restoredOperation = new OSBtreeBucketSetFreeListFirstIndexPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(freeListPageIndex, restoredOperation.getFreeListPageIndex());
    Assert.assertEquals(prevFreeListPageIndex, restoredOperation.getPrevPageIndex());
  }
}
