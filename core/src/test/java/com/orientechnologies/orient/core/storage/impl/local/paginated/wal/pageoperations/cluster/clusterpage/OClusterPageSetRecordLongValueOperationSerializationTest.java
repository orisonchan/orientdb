package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OClusterPageSetRecordLongValueOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 23;
    final int pageIndex = 45;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int recordPosition = 56;
    final int recordOffset = 12;
    final long oldValue = 24;

    OClusterPageSetRecordLongValueOperation operation = new OClusterPageSetRecordLongValueOperation(recordPosition, recordOffset,
        oldValue);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];

    int offset = operation.toStream(stream, 1);
    Assert.assertEquals(serializedSize + 1, offset);

    OClusterPageSetRecordLongValueOperation restoredOperation = new OClusterPageSetRecordLongValueOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(recordPosition, restoredOperation.getRecordPosition());
    Assert.assertEquals(recordOffset, restoredOperation.getRecordOffset());
    Assert.assertEquals(oldValue, restoredOperation.getOldValue());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 23;
    final int pageIndex = 45;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int recordPosition = 56;
    final int recordOffset = 12;
    final long oldValue = 24;

    OClusterPageSetRecordLongValueOperation operation = new OClusterPageSetRecordLongValueOperation(recordPosition, recordOffset,
        oldValue);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());

    buffer.position(1);
    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPageSetRecordLongValueOperation restoredOperation = new OClusterPageSetRecordLongValueOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(recordPosition, restoredOperation.getRecordPosition());
    Assert.assertEquals(recordOffset, restoredOperation.getRecordOffset());
    Assert.assertEquals(oldValue, restoredOperation.getOldValue());
  }
}
