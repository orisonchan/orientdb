package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.brtree.btreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketRemoveNonLeafEntryPageOperation;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OSBTreeBucketRemoveNonLeafEntryPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 456;
    final int pageIndex = 345;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 34;
    final byte[] key = new byte[23];
    final Random random = new Random();
    random.nextBytes(key);

    final int prevChildPointer = 43;
    final int leftNeighbour = 7;
    final int rightNeighbour = 71;

    OSBTreeBucketRemoveNonLeafEntryPageOperation operation = new OSBTreeBucketRemoveNonLeafEntryPageOperation(index, key,
        prevChildPointer, leftNeighbour, rightNeighbour);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OSBTreeBucketRemoveNonLeafEntryPageOperation restoredOperation = new OSBTreeBucketRemoveNonLeafEntryPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertArrayEquals(key, restoredOperation.getKey());
    Assert.assertEquals(prevChildPointer, restoredOperation.getPrevChildPointer());
    Assert.assertEquals(leftNeighbour, restoredOperation.getLeftNeighbour());
    Assert.assertEquals(rightNeighbour, restoredOperation.getRightNeighbour());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 456;
    final int pageIndex = 345;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 34;
    final byte[] key = new byte[23];
    final Random random = new Random();
    random.nextBytes(key);

    final int prevChildPointer = 43;
    final int leftNeighbour = 7;
    final int rightNeighbour = 71;

    OSBTreeBucketRemoveNonLeafEntryPageOperation operation = new OSBTreeBucketRemoveNonLeafEntryPageOperation(index, key,
        prevChildPointer, leftNeighbour, rightNeighbour);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBucketRemoveNonLeafEntryPageOperation restoredOperation = new OSBTreeBucketRemoveNonLeafEntryPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertArrayEquals(key, restoredOperation.getKey());
    Assert.assertEquals(prevChildPointer, restoredOperation.getPrevChildPointer());
    Assert.assertEquals(leftNeighbour, restoredOperation.getLeftNeighbour());
    Assert.assertEquals(rightNeighbour, restoredOperation.getRightNeighbour());
  }
}
