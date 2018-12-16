package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OClusterPageAppendRecordOperationSerializationTest {
  @Test
  public void testSerializeStream() {
    final int recordVersion = 12;

    final byte[] record = new byte[14];
    final Random random = new Random();
    random.nextBytes(record);

    final int index = 5;
    final int pageIndex = 34;
    final int fileId = 12;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final OClusterPageAppendRecordOperation operation = new OClusterPageAppendRecordOperation(recordVersion, record, index);

    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];

    int offset = operation.toStream(stream, 1);
    Assert.assertEquals(serializedSize + 1, offset);

    final OClusterPageAppendRecordOperation restoredOperation = new OClusterPageAppendRecordOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(recordVersion, restoredOperation.getRecordVersion());
    Assert.assertArrayEquals(record, restoredOperation.getRecord());
  }

  @Test
  public void testSerializeByteBuffer() {
    final int recordVersion = 12;

    final byte[] record = new byte[14];
    final Random random = new Random();
    random.nextBytes(record);

    final int index = 5;
    final int pageIndex = 34;
    final int fileId = 12;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final OClusterPageAppendRecordOperation operation = new OClusterPageAppendRecordOperation(recordVersion, record, index);

    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());

    buffer.position(1);
    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OClusterPageAppendRecordOperation restoredOperation = new OClusterPageAppendRecordOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(recordVersion, restoredOperation.getRecordVersion());
    Assert.assertArrayEquals(record, restoredOperation.getRecord());
  }
}
