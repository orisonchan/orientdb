package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OClusterPageReplaceRecordOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 23;
    final int pageIndex = 5;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 34;

    final Random random = new Random();
    final int oldRecordVersion = 25;

    final byte[] oldRecord = new byte[24];
    random.nextBytes(oldRecord);

    OClusterPageReplaceRecordOperation operation = new OClusterPageReplaceRecordOperation(index, oldRecordVersion, oldRecord);

    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OClusterPageReplaceRecordOperation restoredOperation = new OClusterPageReplaceRecordOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(oldRecordVersion, restoredOperation.getOldRecordVersion());
    Assert.assertArrayEquals(oldRecord, restoredOperation.getOldRecord());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 23;
    final int pageIndex = 5;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 34;

    final Random random = new Random();
    final int oldRecordVersion = 25;

    final byte[] oldRecord = new byte[24];
    random.nextBytes(oldRecord);

    OClusterPageReplaceRecordOperation operation = new OClusterPageReplaceRecordOperation(index, oldRecordVersion, oldRecord);

    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPageReplaceRecordOperation restoredOperation = new OClusterPageReplaceRecordOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(oldRecordVersion, restoredOperation.getOldRecordVersion());
    Assert.assertArrayEquals(oldRecord, restoredOperation.getOldRecord());
  }
}
