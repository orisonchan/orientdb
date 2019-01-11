package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.brtree.btreenullbucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreenullbucket.OSBTreeNullBucketSetValuePageOperation;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OSBTreeNullBucketSetValuePageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 345;
    final int pageIndex = 543;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final byte[] value = new byte[12];
    final byte[] prevValue = new byte[34];
    final int valueSize = 56;

    final Random random = new Random();
    random.nextBytes(value);
    random.nextBytes(prevValue);

    OSBTreeNullBucketSetValuePageOperation operation = new OSBTreeNullBucketSetValuePageOperation(prevValue, valueSize);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OSBTreeNullBucketSetValuePageOperation restoredOperation = new OSBTreeNullBucketSetValuePageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertArrayEquals(prevValue, restoredOperation.getPrevValue());
    Assert.assertEquals(valueSize, restoredOperation.getValueSize());
  }

  @Test
  public void testNullStreamSerialization() {
    final int fileId = 345;
    final int pageIndex = 543;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int valueSize = 56;

    final byte[] prevValue = null;

    @SuppressWarnings("ConstantConditions")
    OSBTreeNullBucketSetValuePageOperation operation = new OSBTreeNullBucketSetValuePageOperation(prevValue, valueSize);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OSBTreeNullBucketSetValuePageOperation restoredOperation = new OSBTreeNullBucketSetValuePageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    //noinspection ConstantConditions
    Assert.assertArrayEquals(prevValue, restoredOperation.getPrevValue());
    Assert.assertEquals(valueSize, restoredOperation.getValueSize());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 345;
    final int pageIndex = 543;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final byte[] prevValue = new byte[34];
    final int valueSize = 56;

    final Random random = new Random();
    random.nextBytes(prevValue);

    OSBTreeNullBucketSetValuePageOperation operation = new OSBTreeNullBucketSetValuePageOperation(prevValue, valueSize);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeNullBucketSetValuePageOperation restoredOperation = new OSBTreeNullBucketSetValuePageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertArrayEquals(prevValue, restoredOperation.getPrevValue());
    Assert.assertEquals(valueSize, restoredOperation.getValueSize());
  }

  @Test
  public void testNullBufferSerialization() {
    final int fileId = 345;
    final int pageIndex = 543;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int valueSize = 56;

    final byte[] prevValue = null;

    @SuppressWarnings("ConstantConditions")
    OSBTreeNullBucketSetValuePageOperation operation = new OSBTreeNullBucketSetValuePageOperation(prevValue, valueSize);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeNullBucketSetValuePageOperation restoredOperation = new OSBTreeNullBucketSetValuePageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    //noinspection ConstantConditions
    Assert.assertArrayEquals(prevValue, restoredOperation.getPrevValue());
    Assert.assertEquals(valueSize, restoredOperation.getValueSize());
  }
}
