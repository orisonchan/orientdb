package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OClusterPageNewPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 34;
    final int pageIndex = 56;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    OClusterPageNewPageOperation operation = new OClusterPageNewPageOperation();
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializationSize = operation.serializedSize();
    final byte[] stream = new byte[serializationSize + 1];

    int offset = operation.toStream(stream, 1);
    Assert.assertEquals(serializationSize + 1, offset);

    OClusterPageNewPageOperation restoredOperation = new OClusterPageNewPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializationSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 34;
    final int pageIndex = 56;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    OClusterPageNewPageOperation operation = new OClusterPageNewPageOperation();
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializationSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializationSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializationSize + 1, buffer.position());

    OClusterPageNewPageOperation restoredOperation = new OClusterPageNewPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializationSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
  }
}
