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
package com.orientechnologies.orient.core.storage.index.engine;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.index.OIndexAbstractCursor;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.index.OIndexUpdateAction;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashFunction;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashIndexBucket;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OLocalHashTable;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OMurmurHash3HashFunction;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OSHA256HashFunction;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 15.07.13
 */
public final class OHashTableIndexEngine implements OIndexEngine {
  public static final int VERSION = 2;

  public static final String METADATA_FILE_EXTENSION    = ".him";
  public static final String TREE_FILE_EXTENSION        = ".hit";
  public static final String BUCKET_FILE_EXTENSION      = ".hib";
  public static final String NULL_BUCKET_FILE_EXTENSION = ".hnb";

  private final OLocalHashTable<Object, Object> hashTable;
  private final AtomicLong                      bonsaiFileId = new AtomicLong(0);

  private final int version;

  private final String name;

  public OHashTableIndexEngine(final String name, final OAbstractPaginatedStorage storage, final int version) {
    this.version = version;
    if (version < 2) {
      throw new IllegalStateException("Unsupported version of hash index");
    } else {
      hashTable = new OLocalHashTable<>(name, METADATA_FILE_EXTENSION, TREE_FILE_EXTENSION, BUCKET_FILE_EXTENSION,
          NULL_BUCKET_FILE_EXTENSION, storage);
    }

    this.name = name;
  }

  @Override
  public void init(final String indexName, final String indexType, final OIndexDefinition indexDefinition,
      final boolean isAutomatic, final ODocument metadata) {
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void create(final OBinarySerializer valueSerializer, final boolean isAutomatic, final OType[] keyTypes,
      final boolean nullPointerSupport, final OBinarySerializer keySerializer, final int keySize, final Set<String> clustersToIndex,
      final Map<String, String> engineProperties, final ODocument metadata, final OEncryption encryption) throws IOException {
    final OHashFunction<Object> hashFunction;

    if (encryption != null) {
      //noinspection unchecked
      hashFunction = new OSHA256HashFunction<>(keySerializer);
    } else {
      //noinspection unchecked
      hashFunction = new OMurmurHash3HashFunction<>(keySerializer);
    }

    //noinspection unchecked
    hashTable.create(keySerializer, valueSerializer, keyTypes, encryption, hashFunction, nullPointerSupport);
  }

  @Override
  public void flush() {
  }

  @Override
  public void deleteWithoutLoad(final String indexName) throws IOException {
    hashTable.deleteWithoutLoad(indexName);
  }

  @Override
  public String getIndexNameByKey(final Object key) {
    return name;
  }

  @Override
  public void delete() throws IOException {
    hashTable.delete();
  }

  @Override
  public void load(final String indexName, final OBinarySerializer valueSerializer, final boolean isAutomatic,
      final OBinarySerializer keySerializer, final OType[] keyTypes, final boolean nullPointerSupport, final int keySize,
      final Map<String, String> engineProperties, final OEncryption encryption) {

    final OHashFunction<Object> hashFunction;

    if (encryption != null) {
      //noinspection unchecked
      hashFunction = new OSHA256HashFunction<>(keySerializer);
    } else {
      //noinspection unchecked
      hashFunction = new OMurmurHash3HashFunction<>(keySerializer);
    }
    //noinspection unchecked
    hashTable.load(indexName, keyTypes, nullPointerSupport, encryption, hashFunction, keySerializer, valueSerializer);
  }

  @Override
  public boolean contains(final Object key) {
    return hashTable.get(key) != null;
  }

  @Override
  public boolean remove(final Object key) throws IOException {
    return hashTable.remove(key) != null;
  }

  @Override
  public void clear() throws IOException {
    hashTable.clear();
  }

  @Override
  public void close() {
    hashTable.close();
  }

  @Override
  public Object get(final Object key) {
    return hashTable.get(key);
  }

  @Override
  public void put(final Object key, final Object value) throws IOException {
    hashTable.put(key, value);
  }

  @Override
  public void update(final Object key, final OIndexKeyUpdater<Object> updater) throws IOException {
    final Object value = get(key);
    final OIndexUpdateAction<Object> updated = updater.update(value, bonsaiFileId);
    if (updated.isChange()) {
      put(key, updated.getValue());
    } else if (updated.isRemove()) {
      remove(key);
    } else //noinspection StatementWithEmptyBody
      if (updated.isNothing()) {
      //Do nothing
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean validatedPut(final Object key, final OIdentifiable value, final Validator<Object, OIdentifiable> validator)
      throws IOException {
    return hashTable.validatedPut(key, value, (Validator) validator);
  }

  @Override
  public long size(final ValuesTransformer transformer) {
    if (transformer == null) {
      return hashTable.size();
    } else {
      long counter = 0;

      if (hashTable.isNullKeyIsSupported()) {
        final Object nullValue = hashTable.get(null);
        if (nullValue != null) {
          counter += transformer.transformFromValue(nullValue).size();
        }
      }

      final OHashIndexBucket.Entry<Object, Object> firstEntry = hashTable.firstEntry();
      if (firstEntry == null) {
        return counter;
      }

      OHashIndexBucket.Entry<Object, Object>[] entries = hashTable.ceilingEntries(firstEntry.key);

      while (entries.length > 0) {
        for (final OHashIndexBucket.Entry<Object, Object> entry : entries) {
          counter += transformer.transformFromValue(entry.value).size();
        }

        entries = hashTable.higherEntries(entries[entries.length - 1].key);
      }

      return counter;
    }
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }

  @Override
  public OIndexCursor iterateEntriesBetween(final Object rangeFrom, final boolean fromInclusive, final Object rangeTo,
      final boolean toInclusive, final boolean ascSortOrder, final ValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateEntriesBetween");
  }

  @Override
  public OIndexCursor iterateEntriesMajor(final Object fromKey, final boolean isInclusive, final boolean ascSortOrder,
      final ValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateEntriesMajor");
  }

  @Override
  public OIndexCursor iterateEntriesMinor(final Object toKey, final boolean isInclusive, final boolean ascSortOrder,
      final ValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateEntriesMinor");
  }

  @Override
  public Object getFirstKey() {
    throw new UnsupportedOperationException("firstKey");
  }

  @Override
  public Object getLastKey() {
    throw new UnsupportedOperationException("lastKey");
  }

  @Override
  public OIndexCursor cursor(final ValuesTransformer valuesTransformer) {
    return new OIndexAbstractCursor() {
      private int nextEntriesIndex;
      private OHashIndexBucket.Entry<Object, Object>[] entries;

      private Iterator<OIdentifiable> currentIterator = new OEmptyIterator<>();
      private Object currentKey;

      {
        final OHashIndexBucket.Entry<Object, Object> firstEntry = hashTable.firstEntry();
        if (firstEntry == null) {
          //noinspection unchecked
          entries = OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
        } else {
          entries = hashTable.ceilingEntries(firstEntry.key);
        }

        if (entries.length == 0) {
          currentIterator = null;
        }
      }

      @Override
      public Map.Entry<Object, OIdentifiable> nextEntry() {
        if (currentIterator == null) {
          return null;
        }

        if (currentIterator.hasNext()) {
          return nextCursorValue();
        }

        while (currentIterator != null && !currentIterator.hasNext()) {
          if (entries.length == 0) {
            currentIterator = null;
            return null;
          }

          final OHashIndexBucket.Entry<Object, Object> bucketEntry = entries[nextEntriesIndex];

          currentKey = bucketEntry.key;

          final Object value = bucketEntry.value;
          if (valuesTransformer != null) {
            currentIterator = valuesTransformer.transformFromValue(value).iterator();
          } else {
            currentIterator = Collections.singletonList((OIdentifiable) value).iterator();
          }

          nextEntriesIndex++;

          if (nextEntriesIndex >= entries.length) {
            entries = hashTable.higherEntries(entries[entries.length - 1].key);

            nextEntriesIndex = 0;
          }
        }

        currentIterator = null;
        return null;
      }

      private Map.Entry<Object, OIdentifiable> nextCursorValue() {
        final OIdentifiable identifiable = currentIterator.next();

        return new Map.Entry<Object, OIdentifiable>() {
          @Override
          public Object getKey() {
            return currentKey;
          }

          @Override
          public OIdentifiable getValue() {
            return identifiable;
          }

          @Override
          public OIdentifiable setValue(final OIdentifiable value) {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  @Override
  public OIndexCursor descCursor(final ValuesTransformer valuesTransformer) {
    return new OIndexAbstractCursor() {
      private int nextEntriesIndex;
      private OHashIndexBucket.Entry<Object, Object>[] entries;

      private Iterator<OIdentifiable> currentIterator = new OEmptyIterator<>();
      private Object currentKey;

      {
        final OHashIndexBucket.Entry<Object, Object> lastEntry = hashTable.lastEntry();
        if (lastEntry == null) {
          //noinspection unchecked
          entries = OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
        } else {
          entries = hashTable.floorEntries(lastEntry.key);
        }

        if (entries.length == 0) {
          currentIterator = null;
        }
      }

      @Override
      public Map.Entry<Object, OIdentifiable> nextEntry() {
        if (currentIterator == null) {
          return null;
        }

        if (currentIterator.hasNext()) {
          return nextCursorValue();
        }

        while (currentIterator != null && !currentIterator.hasNext()) {
          if (entries.length == 0) {
            currentIterator = null;
            return null;
          }

          final OHashIndexBucket.Entry<Object, Object> bucketEntry = entries[nextEntriesIndex];

          currentKey = bucketEntry.key;

          final Object value = bucketEntry.value;
          if (valuesTransformer != null) {
            currentIterator = valuesTransformer.transformFromValue(value).iterator();
          } else {
            currentIterator = Collections.singletonList((OIdentifiable) value).iterator();
          }

          nextEntriesIndex--;

          if (nextEntriesIndex < 0) {
            entries = hashTable.lowerEntries(entries[0].key);

            nextEntriesIndex = entries.length - 1;
          }
        }

        currentIterator = null;
        return null;
      }

      private Map.Entry<Object, OIdentifiable> nextCursorValue() {
        final OIdentifiable identifiable = currentIterator.next();

        return new Map.Entry<Object, OIdentifiable>() {
          @Override
          public Object getKey() {
            return currentKey;
          }

          @Override
          public OIdentifiable getValue() {
            return identifiable;
          }

          @Override
          public OIdentifiable setValue(final OIdentifiable value) {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    return new OIndexKeyCursor() {
      private int nextEntriesIndex;
      private OHashIndexBucket.Entry<Object, Object>[] entries;

      {
        final OHashIndexBucket.Entry<Object, Object> firstEntry = hashTable.firstEntry();
        if (firstEntry == null) {
          //noinspection unchecked
          entries = OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
        } else {
          entries = hashTable.ceilingEntries(firstEntry.key);
        }
      }

      @Override
      public Object next(final int prefetchSize) {
        if (entries.length == 0) {
          return null;
        }

        final OHashIndexBucket.Entry<Object, Object> bucketEntry = entries[nextEntriesIndex];
        nextEntriesIndex++;
        if (nextEntriesIndex >= entries.length) {
          entries = hashTable.higherEntries(entries[entries.length - 1].key);

          nextEntriesIndex = 0;
        }

        return bucketEntry.key;
      }
    };
  }

  @Override
  public boolean acquireAtomicExclusiveLock(final Object key) {
    hashTable.acquireAtomicExclusiveLock();
    return true;
  }

}
