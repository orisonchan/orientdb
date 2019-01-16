/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.common.util.OResettable;
import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OAutoConvertToRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeListener;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDelegate;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORidBagDeleteSerializationOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORidBagUpdateSerializationOperation;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiLocal;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Persistent Set<OIdentifiable> implementation that uses the SBTree to handle entries in persistent way.
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public final class OSBTreeRidBag implements ORidBagDelegate {
  private final OSBTreeCollectionManager                           collectionManager = ODatabaseRecordThreadLocal.instance().get()
      .getSbTreeCollectionManager();
  private final NavigableMap<OIdentifiable, Change>                changes           = new ConcurrentSkipListMap<>();
  /**
   * Entries with not valid id.
   */
  private final IdentityHashMap<OIdentifiable, OModifiableInteger> newEntries        = new IdentityHashMap<>();
  private       OBonsaiCollectionPointer                           collectionPointer;
  private       int                                                size;

  private boolean autoConvertToRecord = true;

  private           List<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> changeListeners;
  private transient ORecord                                                       owner;

  @Override
  public final void setSize(final int size) {
    this.size = size;
  }

  private static final class OIdentifiableIntegerEntry implements Entry<OIdentifiable, Integer> {
    private final Entry<OIdentifiable, Integer> entry;
    private final int                           newValue;

    OIdentifiableIntegerEntry(final Entry<OIdentifiable, Integer> entry, final int newValue) {
      this.entry = entry;
      this.newValue = newValue;
    }

    @Override
    public final OIdentifiable getKey() {
      return entry.getKey();
    }

    @Override
    public final Integer getValue() {
      return newValue;
    }

    @Override
    public final Integer setValue(final Integer value) {
      throw new UnsupportedOperationException();
    }
  }

  private final class RIDBagIterator implements Iterator<OIdentifiable>, OResettable, OSizeable, OAutoConvertToRecord {
    private final NavigableMap<OIdentifiable, Change>                    changedValues;
    private final SBTreeMapEntryIterator                                 sbTreeIterator;
    private       boolean                                                convertToRecord;
    private       Iterator<Map.Entry<OIdentifiable, OModifiableInteger>> newEntryIterator;
    private       Iterator<Map.Entry<OIdentifiable, Change>>             changedValuesIterator;
    private       Map.Entry<OIdentifiable, Change>                       nextChange;
    private       Map.Entry<OIdentifiable, Integer>                      nextSBTreeEntry;
    private       OIdentifiable                                          currentValue;
    private       int                                                    currentFinalCounter;
    private       int                                                    currentCounter;
    private       boolean                                                currentRemoved;

    private RIDBagIterator(final IdentityHashMap<OIdentifiable, OModifiableInteger> newEntries,
        final NavigableMap<OIdentifiable, Change> changedValues, final SBTreeMapEntryIterator sbTreeIterator,
        final boolean convertToRecord) {
      newEntryIterator = newEntries.entrySet().iterator();
      this.changedValues = changedValues;
      this.convertToRecord = convertToRecord;
      this.changedValuesIterator = changedValues.entrySet().iterator();
      this.sbTreeIterator = sbTreeIterator;

      nextChange = nextChangedNotRemovedEntry(changedValuesIterator);

      if (sbTreeIterator != null) {
        nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
      }
    }

    @Override
    public boolean hasNext() {
      return newEntryIterator.hasNext() || nextChange != null || nextSBTreeEntry != null || (currentValue != null
          && currentCounter < currentFinalCounter);
    }

    @Override
    public OIdentifiable next() {
      currentRemoved = false;
      if (currentCounter < currentFinalCounter) {
        currentCounter++;
        return currentValue;
      }

      if (newEntryIterator.hasNext()) {
        final Map.Entry<OIdentifiable, OModifiableInteger> entry = newEntryIterator.next();
        currentValue = entry.getKey();
        currentFinalCounter = entry.getValue().intValue();
        currentCounter = 1;
        return currentValue;
      }

      if (nextChange != null && nextSBTreeEntry != null) {
        if (nextChange.getKey().compareTo(nextSBTreeEntry.getKey()) < 0) {
          currentValue = nextChange.getKey();
          currentFinalCounter = nextChange.getValue().applyTo(0);
          currentCounter = 1;

          nextChange = nextChangedNotRemovedEntry(changedValuesIterator);
        } else {
          currentValue = nextSBTreeEntry.getKey();
          currentFinalCounter = nextSBTreeEntry.getValue();
          currentCounter = 1;

          nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
          if (nextChange != null && nextChange.getKey().equals(currentValue)) {
            nextChange = nextChangedNotRemovedEntry(changedValuesIterator);
          }
        }
      } else if (nextChange != null) {
        currentValue = nextChange.getKey();
        currentFinalCounter = nextChange.getValue().applyTo(0);
        currentCounter = 1;

        nextChange = nextChangedNotRemovedEntry(changedValuesIterator);
      } else if (nextSBTreeEntry != null) {
        currentValue = nextSBTreeEntry.getKey();
        currentFinalCounter = nextSBTreeEntry.getValue();
        currentCounter = 1;

        nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
      } else {
        throw new NoSuchElementException();
      }

      if (convertToRecord) {
        return currentValue.getRecord();
      }

      return currentValue;
    }

    @Override
    public void remove() {
      if (currentRemoved) {
        throw new IllegalStateException("Current element has already been removed");
      }

      if (currentValue == null) {
        throw new IllegalStateException("Next method was not called for given iterator");
      }

      if (removeFromNewEntries(currentValue)) {
        if (size >= 0) {
          size--;
        }
      } else {
        final Change counter = changedValues.get(currentValue);
        if (counter != null) {
          counter.decrement();
          if (size >= 0) {
            if (counter.isUndefined()) {
              size = -1;
            } else {
              size--;
            }
          }
        } else {
          if (nextChange != null) {
            changedValues.put(currentValue, new DiffChange(-1));
            changedValuesIterator = changedValues.tailMap(nextChange.getKey(), false).entrySet().iterator();
          } else {
            changedValues.put(currentValue, new DiffChange(-1));
          }

          size = -1;
        }
      }

      if (OSBTreeRidBag.this.owner != null) {
        ORecordInternal.unTrack(OSBTreeRidBag.this.owner, currentValue);
      }

      fireCollectionChangedEvent(
          new OMultiValueChangeEvent<>(OMultiValueChangeEvent.OChangeType.REMOVE, currentValue, null, currentValue, false));
      currentRemoved = true;
    }

    @Override
    public void reset() {
      newEntryIterator = newEntries.entrySet().iterator();

      this.changedValuesIterator = changedValues.entrySet().iterator();
      if (sbTreeIterator != null) {
        this.sbTreeIterator.reset();
      }

      nextChange = nextChangedNotRemovedEntry(changedValuesIterator);

      if (sbTreeIterator != null) {
        nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
      }
    }

    @Override
    public int size() {
      return OSBTreeRidBag.this.size();
    }

    @Override
    public boolean isAutoConvertToRecord() {
      return convertToRecord;
    }

    @Override
    public void setAutoConvertToRecord(final boolean convertToRecord) {
      this.convertToRecord = convertToRecord;
    }

    private Map.Entry<OIdentifiable, Change> nextChangedNotRemovedEntry(final Iterator<Map.Entry<OIdentifiable, Change>> iterator) {
      Map.Entry<OIdentifiable, Change> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        // TODO workaround
        if (entry.getValue().applyTo(0) > 0) {
          return entry;
        }
      }

      return null;
    }
  }

  private final class SBTreeMapEntryIterator implements Iterator<Map.Entry<OIdentifiable, Integer>>, OResettable {
    private final int                                           prefetchSize;
    private       LinkedList<Map.Entry<OIdentifiable, Integer>> preFetchedValues;
    private       OIdentifiable                                 firstKey;

    SBTreeMapEntryIterator(@SuppressWarnings("SameParameterValue") final int prefetchSize) {
      this.prefetchSize = prefetchSize;

      init();
    }

    @Override
    public boolean hasNext() {
      return preFetchedValues != null;
    }

    @Override
    public Map.Entry<OIdentifiable, Integer> next() {
      final Map.Entry<OIdentifiable, Integer> entry = preFetchedValues.removeFirst();
      if (preFetchedValues.isEmpty()) {
        prefetchData(false);
      }

      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
      init();
    }

    private void prefetchData(final boolean firstTime) {
      final OSBTreeBonsai<OIdentifiable, Integer> tree = loadTree();
      if (tree == null) {
        throw new IllegalStateException("RidBag is not properly initialized, can not load tree implementation");
      }

      try {
        tree.loadEntriesMajor(firstKey, firstTime, true, entry -> {
          preFetchedValues.add(new Entry<OIdentifiable, Integer>() {
            @Override
            public OIdentifiable getKey() {
              return entry.getKey();
            }

            @Override
            public Integer getValue() {
              return entry.getValue();
            }

            @Override
            public Integer setValue(final Integer v) {
              throw new UnsupportedOperationException("setValue");
            }
          });

          return preFetchedValues.size() <= prefetchSize;
        });
      } finally {
        releaseTree();
      }

      if (preFetchedValues.isEmpty()) {
        preFetchedValues = null;
      } else {
        firstKey = preFetchedValues.getLast().getKey();
      }
    }

    private void init() {
      final OSBTreeBonsai<OIdentifiable, Integer> tree = loadTree();
      if (tree == null) {
        throw new IllegalStateException("RidBag is not properly initialized, can not load tree implementation");
      }

      try {
        firstKey = tree.firstKey();
      } finally {
        releaseTree();
      }

      if (firstKey == null) {
        this.preFetchedValues = null;
        return;
      }

      this.preFetchedValues = new LinkedList<>();
      prefetchData(true);
    }
  }

  public OSBTreeRidBag(final OBonsaiCollectionPointer pointer, final Map<OIdentifiable, Change> changes) {
    this.collectionPointer = pointer;
    this.changes.putAll(changes);
    this.size = -1;
  }

  public OSBTreeRidBag() {
    collectionPointer = null;
  }

  @Override
  public final ORecord getOwner() {
    return owner;
  }

  @Override
  public final void setOwner(final ORecord owner) {
    if (owner != null && this.owner != null && !this.owner.equals(owner)) {
      throw new IllegalStateException("This data structure is owned by document " + owner
          + " if you want to use it in other document create new rid bag instance and copy content of current one.");
    }
    if (this.owner != null) {
      for (final OIdentifiable entry : newEntries.keySet()) {
        ORecordInternal.unTrack(this.owner, entry);
      }
      for (final OIdentifiable entry : changes.keySet()) {
        ORecordInternal.unTrack(this.owner, entry);
      }
    }

    this.owner = owner;
    if (this.owner != null) {
      for (final OIdentifiable entry : newEntries.keySet()) {
        ORecordInternal.track(this.owner, entry);
      }
      for (final OIdentifiable entry : changes.keySet()) {
        ORecordInternal.track(this.owner, entry);
      }
    }
  }

  @Override
  public final Iterator<OIdentifiable> iterator() {
    return new RIDBagIterator(new IdentityHashMap<>(newEntries), changes,
        collectionPointer != null ? new SBTreeMapEntryIterator(1000) : null, autoConvertToRecord);
  }

  @Override
  public final Iterator<OIdentifiable> rawIterator() {
    return new RIDBagIterator(new IdentityHashMap<>(newEntries), changes,
        collectionPointer != null ? new SBTreeMapEntryIterator(1000) : null, false);
  }

  @Override
  public final void convertLinks2Records() {
    final TreeMap<OIdentifiable, Change> newChanges = new TreeMap<>();
    for (final Map.Entry<OIdentifiable, Change> entry : changes.entrySet()) {
      final OIdentifiable key = entry.getKey().getRecord();
      if (key != null && this.owner != null) {
        ORecordInternal.unTrack(this.owner, entry.getKey());
        ORecordInternal.track(this.owner, key);
      }
      newChanges.put((key == null) ? entry.getKey() : key, entry.getValue());
    }

    changes.clear();
    changes.putAll(newChanges);
  }

  @Override
  public final boolean convertRecords2Links() {
    final Map<OIdentifiable, Change> newChangedValues = new HashMap<>(16);
    for (final Map.Entry<OIdentifiable, Change> entry : changes.entrySet()) {
      newChangedValues.put(entry.getKey().getIdentity(), entry.getValue());
    }

    for (final Map.Entry<OIdentifiable, Change> entry : newChangedValues.entrySet()) {
      if (entry.getKey() instanceof ORecord) {
        final ORecord record = (ORecord) entry.getKey();

        newChangedValues.put(record, entry.getValue());
      } else {
        return false;
      }
    }

    newEntries.clear();

    changes.clear();
    changes.putAll(newChangedValues);

    return true;
  }

  public void mergeChanges(final OSBTreeRidBag treeRidBag) {
    for (final Map.Entry<OIdentifiable, OModifiableInteger> entry : treeRidBag.newEntries.entrySet()) {
      mergeDiffEntry(entry.getKey(), entry.getValue().getValue());
    }

    for (final Map.Entry<OIdentifiable, Change> entry : treeRidBag.changes.entrySet()) {
      final OIdentifiable rec = entry.getKey();
      final Change change = entry.getValue();
      final int diff;
      if (change instanceof DiffChange) {
        diff = change.getValue();
      } else if (change instanceof AbsoluteChange) {
        diff = change.getValue() - getAbsoluteValue(rec).getValue();
      } else {
        throw new IllegalArgumentException("change type is not supported");
      }

      mergeDiffEntry(rec, diff);
    }
  }

  @Override
  public final boolean isAutoConvertToRecord() {
    return autoConvertToRecord;
  }

  @Override
  public final void setAutoConvertToRecord(final boolean convertToRecord) {
    autoConvertToRecord = convertToRecord;
  }

  @Override
  public final boolean detach() {
    return convertRecords2Links();
  }

  @Override
  public final void addAll(final Collection<OIdentifiable> values) {
    for (final OIdentifiable identifiable : values) {
      add(identifiable);
    }
  }

  @Override
  public final void add(final OIdentifiable identifiable) {
    if (identifiable == null) {
      throw new IllegalArgumentException("Impossible to add a null identifiable in a ridbag");
    }

    if (identifiable.getIdentity().isValid()) {
      Change counter = changes.get(identifiable);
      if (counter == null) {
        changes.put(identifiable, new DiffChange(1));
      } else {
        if (counter.isUndefined()) {
          counter = getAbsoluteValue(identifiable);
          changes.put(identifiable, counter);
        }
        counter.increment();
      }
    } else {
      final OModifiableInteger counter = newEntries.get(identifiable);
      if (counter == null) {
        newEntries.put(identifiable, new OModifiableInteger(1));
      } else {
        counter.increment();
      }
    }

    if (size >= 0) {
      size++;
    }

    if (this.owner != null) {
      ORecordInternal.track(this.owner, identifiable);
    }

    fireCollectionChangedEvent(
        new OMultiValueChangeEvent<>(OMultiValueChangeEvent.OChangeType.ADD, identifiable, identifiable, null, false));
  }

  @Override
  public final void remove(final OIdentifiable identifiable) {
    if (removeFromNewEntries(identifiable)) {
      if (size >= 0) {
        size--;
      }
    } else {
      final Change counter = changes.get(identifiable);
      if (counter == null) {
        // Not persistent keys can only be in changes or newEntries
        if (identifiable.getIdentity().isPersistent()) {
          changes.put(identifiable, new DiffChange(-1));
          size = -1;
        } else
          // Return immediately to prevent firing of event
        {
          return;
        }
      } else {
        counter.decrement();

        if (size >= 0) {
          if (counter.isUndefined()) {
            size = -1;
          } else {
            size--;
          }
        }
      }
    }

    if (this.owner != null) {
      ORecordInternal.unTrack(this.owner, identifiable);
    }

    fireCollectionChangedEvent(
        new OMultiValueChangeEvent<>(OMultiValueChangeEvent.OChangeType.REMOVE, identifiable, null, identifiable, false));
  }

  @Override
  public final boolean contains(final OIdentifiable identifiable) {
    if (newEntries.containsKey(identifiable)) {
      return true;
    }

    Change counter = changes.get(identifiable);

    if (counter != null) {
      final AbsoluteChange absoluteValue = getAbsoluteValue(identifiable);

      if (counter.isUndefined()) {
        changes.put(identifiable, absoluteValue);
      }

      counter = absoluteValue;
    } else {
      counter = getAbsoluteValue(identifiable);
    }

    return counter.applyTo(0) > 0;
  }

  @Override
  public final int size() {
    if (size >= 0) {
      return size;
    } else {
      return updateSize();
    }
  }

  @Override
  public final String toString() {
    if (size >= 0) {
      return "[size=" + size + "]";
    }

    return "[...]";
  }

  @Override
  public final boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public final void addChangeListener(final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    if (changeListeners == null) {
      changeListeners = new LinkedList<>();
    }
    changeListeners.add(changeListener);
  }

  @Override
  public final void removeRecordChangeListener(final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    if (changeListeners != null) {
      changeListeners.remove(changeListener);
    }
  }

  @Override
  public final Class<?> getGenericClass() {
    return OIdentifiable.class;
  }

  @Override
  public final Object returnOriginalState(final List<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> multiValueChangeEvents) {
    final OSBTreeRidBag reverted = new OSBTreeRidBag();
    for (final OIdentifiable identifiable : this) {
      reverted.add(identifiable);
    }

    final ListIterator<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> listIterator = multiValueChangeEvents
        .listIterator(multiValueChangeEvents.size());

    while (listIterator.hasPrevious()) {
      final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> event = listIterator.previous();
      switch (event.getChangeType()) {
      case ADD:
        reverted.remove(event.getKey());
        break;
      case REMOVE:
        reverted.add(event.getOldValue());
        break;
      default:
        throw new IllegalArgumentException("Invalid change type : " + event.getChangeType());
      }
    }

    return reverted;
  }

  @Override
  public final int getSerializedSize() {
    int result = 2 * OLongSerializer.LONG_SIZE + 3 * OIntegerSerializer.INT_SIZE;
    if (ODatabaseRecordThreadLocal.instance().get().getStorage() instanceof OStorageProxy
        || ORecordSerializationContext.getContext() == null) {
      result += getChangesSerializedSize();
    }
    return result;
  }

  @Override
  public final int getSerializedSize(final byte[] stream, final int offset) {
    return getSerializedSize();
  }

  private void rearrangeChanges() {
    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    for (final Entry<OIdentifiable, Change> change : this.changes.entrySet()) {
      final OIdentifiable key = change.getKey();
      if (db != null && db.getTransaction().isActive()) {
        if (!key.getIdentity().isPersistent()) {
          final OIdentifiable newKey = db.getTransaction().getRecord(key.getIdentity());
          if (newKey != null) {
            changes.remove(key);
            changes.put(newKey, change.getValue());
          }
        }
      }
    }
  }

  public final void handleContextSBTree(final ORecordSerializationContext context, final OBonsaiCollectionPointer pointer) {
    rearrangeChanges();
    this.collectionPointer = pointer;
    context.push(new ORidBagUpdateSerializationOperation(changes, collectionPointer));
  }

  @Override
  public final int serialize(final byte[] stream, int offset, final UUID ownerUuid) {
    applyNewEntries();

    final ORecordSerializationContext context;
    final boolean remoteMode = ODatabaseRecordThreadLocal.instance().get().getStorage() instanceof OStorageProxy;
    if (remoteMode) {
      context = null;
    } else {
      context = ORecordSerializationContext.getContext();
    }

    // make sure that we really save underlying record.
    if (collectionPointer == null) {
      if (context != null) {
        final int clusterId = getHighLevelDocClusterId();
        assert clusterId > -1;
        try {
          collectionPointer = ODatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager()
              .createSBTree(clusterId, ownerUuid);
        } catch (final IOException e) {
          throw OException.wrapException(new ODatabaseException("Error during ridbag creation"), e);
        }
      }
    }

    final OBonsaiCollectionPointer collectionPointer;
    if (this.collectionPointer != null) {
      collectionPointer = this.collectionPointer;
    } else {
      collectionPointer = OBonsaiCollectionPointer.INVALID;
    }

    OLongSerializer.serializeLiteral(collectionPointer.getFileId(), stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    final OBonsaiBucketPointer rootPointer = collectionPointer.getRootPointer();
    OLongSerializer.serializeLiteral(rootPointer.getPageIndex(), stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.serializeLiteral(rootPointer.getPageOffset(), stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    // Keep this section for binary compatibility with versions older then 1.7.5
    OIntegerSerializer.serializeLiteral(size, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    if (context == null) {
      ChangeSerializationHelper.INSTANCE.serializeChanges(changes, OLinkSerializer.INSTANCE, stream, offset);
    } else {
      handleContextSBTree(context, collectionPointer);
      // 0-length serialized list of changes
      OIntegerSerializer.serializeLiteral(0, stream, offset);
      offset += OIntegerSerializer.INT_SIZE;
    }

    return offset;
  }

  public final void applyNewEntries() {
    for (final Entry<OIdentifiable, OModifiableInteger> entry : newEntries.entrySet()) {
      final OIdentifiable identifiable = entry.getKey();
      assert identifiable instanceof ORecord;
      final Change c = changes.get(identifiable);

      final int delta = entry.getValue().intValue();
      if (c == null) {
        changes.put(identifiable, new DiffChange(delta));
      } else {
        c.applyDiff(delta);
      }
    }
    newEntries.clear();
  }

  public void clearChanges() {
    changes.clear();
  }

  @Override
  public final void requestDelete() {
    final ORecordSerializationContext context = ORecordSerializationContext.getContext();
    if (context != null && collectionPointer != null) {
      context.push(new ORidBagDeleteSerializationOperation(collectionPointer, this));
    }
  }

  public void confirmDelete() {
    collectionPointer = null;
    changes.clear();
    newEntries.clear();
    size = 0;
    if (changeListeners != null) {
      changeListeners.clear();
    }
    changeListeners = null;
  }

  @Override
  public final int deserialize(final byte[] stream, int offset) {
    final long fileId = OLongSerializer.deserializeLiteral(stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    final long pageIndex = OLongSerializer.deserializeLiteral(stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    final int pageOffset = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    // Cached bag size. Not used after 1.7.5
    offset += OIntegerSerializer.INT_SIZE;

    if (fileId == -1) {
      collectionPointer = null;
    } else {
      collectionPointer = new OBonsaiCollectionPointer(fileId, new OBonsaiBucketPointer(pageIndex, pageOffset));
    }

    this.size = -1;

    changes.putAll(ChangeSerializationHelper.INSTANCE.deserializeChanges(stream, offset));

    offset += OIntegerSerializer.INT_SIZE + (OLinkSerializer.RID_SIZE + Change.SIZE) * changes.size();

    return offset;
  }

  public OBonsaiCollectionPointer getCollectionPointer() {
    return collectionPointer;
  }

  public void setCollectionPointer(final OBonsaiCollectionPointer collectionPointer) {
    this.collectionPointer = collectionPointer;
  }

  @Override
  public final List<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> getChangeListeners() {
    if (changeListeners == null) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(changeListeners);
  }

  @Override
  public final void fireCollectionChangedEvent(final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> event) {
    if (changeListeners != null) {
      for (final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener : changeListeners) {
        if (changeListener != null) {
          changeListener.onAfterRecordChanged(event);
        }
      }
    }
  }

  private OSBTreeBonsai<OIdentifiable, Integer> loadTree() {
    if (collectionPointer == null) {
      return null;
    }

    return collectionManager.loadSBTree(collectionPointer);
  }

  private void releaseTree() {
    if (collectionPointer == null) {
      return;
    }

    collectionManager.releaseSBTree(collectionPointer);
  }

  private void mergeDiffEntry(final OIdentifiable key, final int diff) {
    if (diff > 0) {
      for (int i = 0; i < diff; i++) {
        add(key);
      }
    } else {
      for (int i = diff; i < 0; i++) {
        remove(key);
      }
    }
  }

  private AbsoluteChange getAbsoluteValue(final OIdentifiable identifiable) {
    final OSBTreeBonsai<OIdentifiable, Integer> tree = loadTree();
    try {
      Integer oldValue;

      if (tree == null) {
        oldValue = 0;
      } else {
        oldValue = tree.get(identifiable);
      }

      if (oldValue == null) {
        oldValue = 0;
      }

      final Change change = changes.get(identifiable);

      return new AbsoluteChange(change == null ? oldValue : change.applyTo(oldValue));
    } finally {
      releaseTree();
    }
  }

  /**
   * Recalculates real bag size.
   *
   * @return real size
   */
  private int updateSize() {
    int size = 0;
    if (collectionPointer != null) {
      final OSBTreeBonsai<OIdentifiable, Integer> tree = loadTree();
      if (tree == null) {
        throw new IllegalStateException("RidBag is not properly initialized, can not load tree implementation");
      }

      try {
        size = tree.getRealBagSize(changes);
      } finally {
        releaseTree();
      }
    } else {
      for (final Change change : changes.values()) {
        size += change.applyTo(0);
      }
    }

    for (final OModifiableInteger diff : newEntries.values()) {
      size += diff.getValue();
    }

    this.size = size;
    return size;
  }

  private int getChangesSerializedSize() {
    final Set<OIdentifiable> changedIds = new HashSet<>(changes.keySet());
    changedIds.addAll(newEntries.keySet());
    return ChangeSerializationHelper.INSTANCE.getChangesSerializedSize(changedIds.size());
  }

  private int getHighLevelDocClusterId() {
    ORecordElement owner = this.owner;
    while (owner != null && owner.getOwner() != null) {
      owner = owner.getOwner();
    }

    if (owner != null) {
      return ((OIdentifiable) owner).getIdentity().getClusterId();
    }

    return -1;
  }

  /**
   * Removes entry with given key from {@link #newEntries}.
   *
   * @param identifiable key to remove
   *
   * @return true if entry have been removed
   */
  private boolean removeFromNewEntries(final OIdentifiable identifiable) {
    final OModifiableInteger counter = newEntries.get(identifiable);
    if (counter == null) {
      return false;
    } else {
      if (counter.getValue() == 1) {
        newEntries.remove(identifiable);
      } else {
        counter.decrement();
      }
      return true;
    }
  }

  private Map.Entry<OIdentifiable, Integer> nextChangedNotRemovedSBTreeEntry(
      final Iterator<Map.Entry<OIdentifiable, Integer>> iterator) {
    while (iterator.hasNext()) {
      final Map.Entry<OIdentifiable, Integer> entry = iterator.next();
      final Change change = changes.get(entry.getKey());
      if (change == null) {
        return entry;
      }

      final int newValue = change.applyTo(entry.getValue());

      if (newValue > 0) {
        return new OIdentifiableIntegerEntry(entry, newValue);
      }
    }

    return null;
  }

  public void debugPrint(final PrintStream writer) throws IOException {
    final OSBTreeBonsai<OIdentifiable, Integer> tree = loadTree();
    if (tree instanceof OSBTreeBonsaiLocal) {
      ((OSBTreeBonsaiLocal) tree).debugPrintBucket(writer);
    }
  }

  @Override
  public final NavigableMap<OIdentifiable, Change> getChanges() {
    applyNewEntries();
    return changes;
  }

  @Override
  public final void replace(final OMultiValueChangeEvent<Object, Object> event, final Object newValue) {
    //do nothing not needed
  }

}
