package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.brtree.btreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketSetRightSiblingPageOperation;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OSBTreeBucketSetRightSiblingPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 345;
    final int pageIndex = 543;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int rightSibling = 12;
    final int prevRightSibling = 21;

    OSBTreeBucketSetRightSiblingPageOperation operation = new OSBTreeBucketSetRightSiblingPageOperation(rightSibling,
        prevRightSibling);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OSBTreeBucketSetRightSiblingPageOperation restoredOperation = new OSBTreeBucketSetRightSiblingPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(prevRightSibling, restoredOperation.getPrevRightSibling());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 345;
    final int pageIndex = 543;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int rightSibling = 12;
    final int prevRightSibling = 21;

    OSBTreeBucketSetRightSiblingPageOperation operation = new OSBTreeBucketSetRightSiblingPageOperation(rightSibling,
        prevRightSibling);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBucketSetRightSiblingPageOperation restoredOperation = new OSBTreeBucketSetRightSiblingPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(prevRightSibling, restoredOperation.getPrevRightSibling());
  }
}
