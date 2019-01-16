package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tglman on 15/06/17.
 */
public class ChangeSerializationHelper {
  public static final ChangeSerializationHelper INSTANCE = new ChangeSerializationHelper();

  public static Change createChangeInstance(final byte type, final int value) {
    switch (type) {
    case AbsoluteChange.TYPE:
      return new AbsoluteChange(value);
    case DiffChange.TYPE:
      return new DiffChange(value);
    default:
      throw new IllegalArgumentException("Change type is incorrect");
    }
  }

  public Change deserializeChange(final byte[] stream, final int offset) {
    final int value = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, offset + OByteSerializer.BYTE_SIZE);
    return createChangeInstance(OByteSerializer.deserializeLiteral(stream, offset), value);
  }

  public Map<OIdentifiable, Change> deserializeChanges(final byte[] stream, int offset) {
    final int count = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final HashMap<OIdentifiable, Change> res = new HashMap<>();
    for (int i = 0; i < count; i++) {
      final ORecordId rid = OLinkSerializer.INSTANCE.deserialize(stream, offset);
      offset += OLinkSerializer.RID_SIZE;
      final Change change = ChangeSerializationHelper.INSTANCE.deserializeChange(stream, offset);
      offset += Change.SIZE;

      final OIdentifiable identifiable;
      if (rid.isTemporary() && rid.getRecord() != null)
        identifiable = rid.getRecord();
      else
        identifiable = rid;

      res.put(identifiable, change);
    }

    return res;
  }

  public <K extends OIdentifiable> void serializeChanges(final Map<K, Change> changes, final OBinarySerializer<K> keySerializer,
      final byte[] stream, int offset) {
    OIntegerSerializer.serializeLiteral(changes.size(), stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (final Map.Entry<K, Change> entry : changes.entrySet()) {
      K key = entry.getKey();

      if (key.getIdentity().isTemporary())
        //noinspection unchecked
        key = key.getRecord();

      keySerializer.serialize(key, stream, offset);
      offset += keySerializer.getObjectSize(key);

      offset += entry.getValue().serialize(stream, offset);
    }
  }

  public int getChangesSerializedSize(final int changesCount) {
    return changesCount * (OLinkSerializer.RID_SIZE + Change.SIZE);
  }
}
