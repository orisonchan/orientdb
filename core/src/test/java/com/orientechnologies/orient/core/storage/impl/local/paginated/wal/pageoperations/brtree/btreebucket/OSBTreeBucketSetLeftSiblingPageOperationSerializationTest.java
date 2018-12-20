package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.brtree.btreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketSetLeftSiblingPageOperation;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OSBTreeBucketSetLeftSiblingPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 456;
    final int pageIndex = 34;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int leftSibling = 21;
    final int prevLeftSibling = 12;

    OSBTreeBucketSetLeftSiblingPageOperation operation = new OSBTreeBucketSetLeftSiblingPageOperation(leftSibling, prevLeftSibling);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OSBTreeBucketSetLeftSiblingPageOperation resotredOperation = new OSBTreeBucketSetLeftSiblingPageOperation();
    offset = resotredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, resotredOperation.getFileId());
    Assert.assertEquals(pageIndex, resotredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, resotredOperation.getOperationUnitId());
    Assert.assertEquals(leftSibling, resotredOperation.getLeftSibling());
    Assert.assertEquals(prevLeftSibling, resotredOperation.getPrevLeftSibling());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 456;
    final int pageIndex = 34;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int leftSibling = 21;
    final int prevLeftSibling = 12;

    OSBTreeBucketSetLeftSiblingPageOperation operation = new OSBTreeBucketSetLeftSiblingPageOperation(leftSibling, prevLeftSibling);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBucketSetLeftSiblingPageOperation resotredOperation = new OSBTreeBucketSetLeftSiblingPageOperation();
    int offset = resotredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, resotredOperation.getFileId());
    Assert.assertEquals(pageIndex, resotredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, resotredOperation.getOperationUnitId());
    Assert.assertEquals(leftSibling, resotredOperation.getLeftSibling());
    Assert.assertEquals(prevLeftSibling, resotredOperation.getPrevLeftSibling());
  }

}
