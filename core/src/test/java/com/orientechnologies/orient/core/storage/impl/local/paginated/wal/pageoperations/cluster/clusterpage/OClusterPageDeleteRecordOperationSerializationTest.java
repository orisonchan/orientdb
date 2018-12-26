package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OClusterPageDeleteRecordOperationSerializationTest {
  @Test
  public void testSerializeStream() {
    final int fileId = 34;
    final int pageIndex = 25;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int recordVersion = 12;

    final byte[] record = new byte[23];
    final Random random = new Random();
    random.nextBytes(record);

    OClusterPageDeleteRecordOperation operation = new OClusterPageDeleteRecordOperation(recordVersion, record);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OClusterPageDeleteRecordOperation restoredOperation = new OClusterPageDeleteRecordOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(recordVersion, restoredOperation.getRecordVersion());
    Assert.assertArrayEquals(record, restoredOperation.getRecord());
  }

  @Test
  public void testSerializeBuffer() {
    final int fileId = 34;
    final int pageIndex = 25;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int recordVersion = 12;

    final byte[] record = new byte[23];
    final Random random = new Random();
    random.nextBytes(record);

    OClusterPageDeleteRecordOperation operation = new OClusterPageDeleteRecordOperation(recordVersion, record);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPageDeleteRecordOperation restoredOperation = new OClusterPageDeleteRecordOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(recordVersion, restoredOperation.getRecordVersion());
    Assert.assertArrayEquals(record, restoredOperation.getRecord());
  }

}
