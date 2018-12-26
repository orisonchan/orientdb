package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.brtree.btreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OSBTreeBucketInsertNonLeafKeyNeighboursPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 453;
    final int pageIndex = 345;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 12;
    final int keySize = 34;

    int prevChildPointer = 17;

    OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation operation = new OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation(index,
        keySize, prevChildPointer);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];

    int offset = operation.toStream(stream, 1);
    Assert.assertEquals(serializedSize + 1, offset);

    OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation restoredOperation = new OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(keySize, restoredOperation.getKeySize());
    Assert.assertEquals(prevChildPointer, restoredOperation.getPrevChildPointer());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 453;
    final int pageIndex = 345;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 12;
    final int keySize = 34;
    int prevChildPointer = 17;

    OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation operation = new OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation(index,
        keySize, prevChildPointer);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation restoredOperation = new OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(keySize, restoredOperation.getKeySize());
    Assert.assertEquals(prevChildPointer, restoredOperation.getPrevChildPointer());
  }
}
