package com.orientechnologies.orient.core.storage.index.hashindex.local;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.OTXApprover;
import com.orientechnologies.orient.core.storage.index.sbtree.local.SBTreeRollbackTestIT;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
public class OLocalHashTableRollbackTestIT {
  private static final String           DB_NAME     = "TestDB";
  private static final String           VALUE_INDEX = "ValueIndex";
  private static       ODatabaseSession session;
  private static       OrientDB         orient;
  private static       TestTXApprover   txApprover;

  private static int initialAmountOfRecords;
  private static int testedAmountOfRecords;
  private static int additionalAmountOfRecords;
  private static int iterationsCount;

  private static final String CLASS_NAME = "TestClass";

  @BeforeClass
  public static void beforeClass() {
    final String dbPath =
        System.getProperty("buildDirectory", "./target") + File.separator + SBTreeRollbackTestIT.class.getSimpleName();

    OFileUtils.deleteRecursively(new File(dbPath));
    txApprover = new TestTXApprover();
    OrientDBConfig config = OrientDBConfig.builder().addConfig(OGlobalConfiguration.TX_APPROVER, txApprover).build();

    orient = new OrientDB("plocal:" + dbPath, config);
    orient.create(DB_NAME, ODatabaseType.PLOCAL);

    initialAmountOfRecords = Integer.parseInt(System.getProperty("initialAmountOfRecords", "1000000"));
    testedAmountOfRecords = Integer.parseInt(System.getProperty("testedAmountOfRecords", "100000"));
    additionalAmountOfRecords = Integer.parseInt(System.getProperty("additionalAmountOfRecords", initialAmountOfRecords + ""));
    iterationsCount = Integer.parseInt(System.getProperty("iterationsCount", "1"));
  }

  @AfterClass
  public static void afterClass() {
    orient.drop(DB_NAME);
    orient.close();
  }

  @Before
  public void beforeMethod() {
    txApprover.approve = true;

    session = orient.open(DB_NAME, "admin", "admin");

    createSchema();

  }

  private static void createSchema() {
    final OMetadata metadata = session.getMetadata();
    final OSchema schema = metadata.getSchema();
    OClass clz = schema.createClass(CLASS_NAME);
    clz.createProperty("value", OType.BINARY);
    clz.createIndex(VALUE_INDEX, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "value");
  }

  @After
  public void afterMethod() {
    dropSchema();

    session.close();
  }

  private static void dropSchema() {
    txApprover.approve = true;
    final OMetadata metadata = session.getMetadata();
    final OSchema schema = metadata.getSchema();

    schema.dropClass(CLASS_NAME);
  }

  @Test
  public void testAdditionRollbackOne() {
    for (int k = 0; k < iterationsCount; k++) {
      System.out.printf("Iteration %d out of %d%n", k + 1, iterationsCount);
      long seed = -1;

      try {
        seed = System.nanoTime();

        final Random random = new Random(seed);

        final Map<ORID, byte[]> values = new HashMap<>();

        System.out.println("Loading initial set of records");
        for (int i = 0; i < initialAmountOfRecords; i++) {
          iterateOverAllRecords(values);

          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;

          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, initialAmountOfRecords);
          }
        }

        txApprover.approve = false;
        System.out.println("Loading reverted set of records");
        for (int i = 0; i < testedAmountOfRecords / 10; i++) {
          try {
            session.begin();

            for (int n = 0; n < 10; n++) {
              final ODocument document = new ODocument(CLASS_NAME);
              final int recordSize = random.nextInt(200) + 100;
              final byte[] value = new byte[recordSize];
              random.nextBytes(value);

              ensureValueIsUnique(random, value);

              document.field("value", value);
              document.save();

              if (i > 0 && (i * 10 + n) % 20_000 == 0) {
                System.out.printf("%d records are tested out of %d%n", i * 10 + n, testedAmountOfRecords);
              }
            }

            session.commit();
          } catch (NotApprovedException e) {
            //skip
          }

        }

        session.close();
        session = orient.open(DB_NAME, "admin", "admin");

        txApprover.approve = true;
        System.out.println("Testing initially loaded records");
        OIndex index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        System.out.println("Loading additional set of records");
        for (int i = 0; i < additionalAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, additionalAmountOfRecords);
          }
        }

        session.close();
        session = orient.open("TestDB", "admin", "admin");

        System.out.println("Testing all loaded records");
        index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords + additionalAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        if (k < iterationsCount - 1) {
          dropSchema();
          createSchema();
        }
      } catch (Exception | Error e) {
        System.out.printf("testAdditionRollbackOne seed: %d%n", seed);
        throw e;
      }
    }
  }

  @Test
  public void testAddRollbackTwo() {
    for (int k = 0; k < iterationsCount; k++) {
      System.out.printf("Iteration %d out of %d%n", k + 1, iterationsCount);
      long seed = -1;

      try {
        seed = System.nanoTime();

        final Random random = new Random(seed);

        final Map<ORID, byte[]> values = new HashMap<>();

        for (int i = 0; i < 2 * initialAmountOfRecords; i++) {
          if (i % 2 == 0) {
            txApprover.approve = true;
            final ODocument document = new ODocument(CLASS_NAME);
            final int recordSize = random.nextInt(200) + 100;
            final byte[] value = new byte[recordSize];
            random.nextBytes(value);
            ensureValueIsUnique(random, value);

            document.field("value", value);
            document.save();

            values.put(document.getIdentity(), value);
          } else {
            txApprover.approve = false;
            try {
              session.begin();
              for (int n = 0; n < 10; n++) {
                final ODocument document = new ODocument(CLASS_NAME);
                final int recordSize = random.nextInt(200) + 100;
                final byte[] value = new byte[recordSize];
                random.nextBytes(value);
                ensureValueIsUnique(random, value);

                document.field("value", value);
                document.save();
              }

              session.commit();
            } catch (NotApprovedException e) {
              //continue
            }
          }

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i / 2, initialAmountOfRecords);
          }
        }

        session.close();
        session = orient.open("TestDB", "admin", "admin");

        txApprover.approve = true;
        System.out.println("Testing initially loaded records");
        OIndex index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords, index.getSize());

        iterateOverAllRecords(values);

        System.out.println("Loading additional set of records");
        for (int i = 0; i < additionalAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, additionalAmountOfRecords);
          }
        }

        session.close();
        session = orient.open("TestDB", "admin", "admin");

        System.out.println("Testing all loaded records");
        index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords + additionalAmountOfRecords, index.getSize());

        iterateOverAllRecords(values);
        if (k < iterationsCount - 1) {
          dropSchema();
          createSchema();
        }
      } catch (Exception | Error e) {
        System.out.printf("testAddRollbackTwo seed : %d%n", seed);
        throw e;
      }
    }
  }

  @Test
  public void testUpdateRollbackOne() {
    long seed = -1;

    for (int k = 0; k < iterationsCount; k++) {
      System.out.printf("Iteration %d out of %d%n", k + 1, iterationsCount);
      try {
        seed = System.nanoTime();
        Random random = new Random(seed);

        List<ORID> rids = new ArrayList<>();
        Map<ORID, byte[]> values = new HashMap<>();

        System.out.println("Loading initial set of records");
        for (int i = 0; i < initialAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          rids.add(document.getIdentity());
          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, initialAmountOfRecords);
          }
        }

        System.out.println("Testing rollback of update");
        for (int i = 0; i < testedAmountOfRecords; i++) {
          txApprover.approve = true;

          session.begin();
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();
          session.commit();

          rids.add(document.getIdentity());
          values.put(document.getIdentity(), value);

          txApprover.approve = false;
          try {
            session.begin();
            for (int n = 0; n < 10; n++) {
              final int updateIndex = random.nextInt(rids.size());
              final ORID ridToUpdate = rids.get(updateIndex);
              final int valueSize = random.nextInt(200) + 100;
              final byte[] val = new byte[valueSize];
              ensureValueIsUnique(random, val);

              final ODocument doc = session.load(ridToUpdate);
              Assert.assertNotNull(doc);
              doc.field("value", val);
              doc.save();
            }
            session.commit();
          } catch (NotApprovedException e) {
            //continue
          }

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d iterations are tested out of %d%n", i, testedAmountOfRecords);
          }
        }

        session.close();
        session = orient.open(DB_NAME, "admin", "admin");

        txApprover.approve = true;
        System.out.println("Testing all loaded records");
        OIndex index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords + testedAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        System.out.println("Loading additional set of records");
        for (int i = 0; i < additionalAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, additionalAmountOfRecords);
          }
        }

        session.close();
        session = orient.open(DB_NAME, "admin", "admin");

        System.out.println("Testing all loaded records");
        index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords + testedAmountOfRecords + additionalAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        if (k < iterationsCount - 1) {
          dropSchema();
          createSchema();
        }
      } catch (Error | Exception e) {
        System.out.printf("testUpdateRollbackOne seed: %d%n", seed);
        throw e;
      }
    }
  }

  @Test
  public void testUpdateRollbackTwo() {
    long seed = -1;
    for (int k = 0; k < iterationsCount; k++) {
      System.out.printf("Iteration %d out of %d%n", k + 1, iterationsCount);
      try {
        seed = System.nanoTime();
        Random random = new Random(seed);

        List<ORID> rids = new ArrayList<>();
        Map<ORID, byte[]> values = new HashMap<>();

        System.out.println("Loading initial set of records");
        for (int i = 0; i < initialAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          rids.add(document.getIdentity());
          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, initialAmountOfRecords);
          }
        }

        System.out.println("Testing rollback of update");

        for (int i = 0; i < testedAmountOfRecords; i++) {
          txApprover.approve = true;

          int indexToUpdate = random.nextInt(rids.size());
          ORID ridToUpdate = rids.get(indexToUpdate);
          ODocument doc = session.load(ridToUpdate);
          int valSize = random.nextInt(200) + 100;
          byte[] val = new byte[valSize];
          random.nextBytes(val);
          ensureValueIsUnique(random, val);
          doc.field("value", val);
          doc.save();
          values.put(doc.getIdentity(), val);

          txApprover.approve = false;

          session.begin();
          try {
            for (int n = 0; n < 10; n++) {
              ridToUpdate = rids.get(indexToUpdate);
              doc = session.load(ridToUpdate);
              valSize = random.nextInt(200) + 100;
              val = new byte[valSize];
              ensureValueIsUnique(random, val);
              random.nextBytes(val);
              doc.field("value", val);
              doc.save();
            }

            session.commit();
          } catch (NotApprovedException e) {
            //continue
          }

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d iterations are tested out of %d%n", i, testedAmountOfRecords);
          }
        }

        session.close();
        session = orient.open("TestDB", "admin", "admin");

        txApprover.approve = true;
        System.out.println("Testing all loaded records");
        OIndex index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        System.out.println("Loading additional set of records");
        for (int i = 0; i < additionalAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, additionalAmountOfRecords);
          }
        }

        session.close();
        session = orient.open("TestDB", "admin", "admin");

        System.out.println("Testing all loaded records");
        index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords + additionalAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        if (k < iterationsCount - 1) {
          dropSchema();
          createSchema();
        }
      } catch (Error | Exception e) {
        System.out.printf("testUpdateRollbackTwo seed: %d%n", seed);
        throw e;
      }
    }
  }

  @Test
  public void testDeletionRollbackOne() {
    long seed = -1;
    for (int k = 0; k < iterationsCount; k++) {
      System.out.printf("Iteration %d out of %d%n", k + 1, iterationsCount);
      try {
        seed = System.nanoTime();
        Random random = new Random(seed);

        List<ORID> rids = new ArrayList<>();
        Map<ORID, byte[]> values = new HashMap<>();

        System.out.println("Loading initial set of records");
        for (int i = 0; i < initialAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          rids.add(document.getIdentity());
          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, initialAmountOfRecords);
          }
        }

        System.out.println("Testing rollback of delete");
        for (int i = 0; i < testedAmountOfRecords; i++) {
          txApprover.approve = true;

          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          rids.add(document.getIdentity());
          values.put(document.getIdentity(), value);

          txApprover.approve = false;

          session.begin();
          try {
            Set<ORID> ridsToDelete = new HashSet<>();
            while (ridsToDelete.size() < 10) {
              int ridToDelete = random.nextInt(rids.size());
              ridsToDelete.add(rids.get(ridToDelete));
            }

            for (ORID orid : ridsToDelete) {
              final ODocument doc = session.load(orid);
              Assert.assertNotNull(doc);
              doc.delete();
            }

            session.commit();
          } catch (NotApprovedException e) {
            //continue
          }

          if (i > 0 && i % 10_000 == 0) {
            System.out.printf("%d iterations are tested out of %d%n", i, testedAmountOfRecords);
          }
        }

        session.close();
        session = orient.open("TestDB", "admin", "admin");

        txApprover.approve = true;
        System.out.println("Testing all loaded records");
        OIndex index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords + testedAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        System.out.println("Loading additional set of records");
        for (int i = 0; i < additionalAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, additionalAmountOfRecords);
          }
        }

        session.close();
        session = orient.open("TestDB", "admin", "admin");

        System.out.println("Testing all loaded records");
        index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords + additionalAmountOfRecords + testedAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        if (k < iterationsCount - 1) {
          dropSchema();
          createSchema();
        }
      } catch (Error | Exception e) {
        System.out.printf("testDeletionRollbackOne seed : %d%n", seed);
        throw e;
      }
    }
  }

  @Test
  public void testDeletionRollbackTwo() {
    long seed = -1;
    for (int k = 0; k < iterationsCount; k++) {
      System.out.printf("Iteration %d out of %d%n", k + 1, iterationsCount);
      try {
        seed = System.nanoTime();
        Random random = new Random(seed);

        TreeMap<Integer, ORID> rids = new TreeMap<>();
        Map<ORID, byte[]> values = new HashMap<>();

        System.out.println("Loading initial set of records");
        for (int i = 0; i < initialAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          rids.put(i, document.getIdentity());
          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, initialAmountOfRecords);
          }
        }

        System.out.println("Testing rollback of delete");
        for (int i = 0; i < testedAmountOfRecords; i++) {
          txApprover.approve = true;

          ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          rids.put(initialAmountOfRecords + i, document.getIdentity());
          values.put(document.getIdentity(), value);

          txApprover.approve = false;

          session.begin();
          try {
            Set<ORID> ridsToDelete = new HashSet<>();
            while (ridsToDelete.size() < 10) {
              Integer ridToDelete = rids.ceilingKey(random.nextInt(rids.lastKey()));
              if (ridToDelete == null) {
                ridToDelete = rids.firstKey();
              }

              ridsToDelete.add(rids.get(ridToDelete));
            }

            for (ORID orid : ridsToDelete) {
              final ODocument doc = session.load(orid);
              Assert.assertNotNull(doc);
              doc.delete();
            }

            session.commit();
          } catch (NotApprovedException e) {
            //continue
          }

          txApprover.approve = true;

          Integer ridToDelete = rids.ceilingKey(random.nextInt(rids.lastKey()));
          if (ridToDelete == null) {
            ridToDelete = rids.firstKey();
          }
          document = session.load(rids.get(ridToDelete));
          Assert.assertNotNull(document);
          document.delete();

          rids.remove(ridToDelete);
          values.remove(document.getIdentity());

          if (i > 0 && i % 10_000 == 0) {
            System.out.printf("%d iterations are tested out of %d%n", i, testedAmountOfRecords);
          }
        }

        session.close();
        session = orient.open(DB_NAME, "admin", "admin");

        txApprover.approve = true;
        System.out.println("Testing all loaded records");
        OIndex index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        System.out.println("Loading additional set of records");
        for (int i = 0; i < additionalAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, additionalAmountOfRecords);
          }
        }

        session.close();
        session = orient.open("TestDB", "admin", "admin");

        System.out.println("Testing all loaded records");
        index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords + additionalAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        if (k < iterationsCount - 1) {
          dropSchema();
          createSchema();
        }
      } catch (Error | Exception e) {
        System.out.printf("testDeletionRollbackTwo seed %d%n", seed);
        throw e;
      }
    }
  }

  @Test
  public void testDeletionRollbackThree() {
    long seed = -1;
    for (int k = 0; k < iterationsCount; k++) {
      System.out.printf("Iteration %d out of %d%n", k + 1, iterationsCount);
      try {
        seed = System.nanoTime();
        Random random = new Random(seed);

        TreeMap<Integer, ORID> rids = new TreeMap<>();
        Map<ORID, byte[]> values = new HashMap<>();

        System.out.println("Loading initial set of records");
        for (int i = 0; i < initialAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          rids.put(i, document.getIdentity());
          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, initialAmountOfRecords);
          }
        }

        System.out.println("Testing rollback of delete");
        for (int i = 0; i < testedAmountOfRecords; i++) {
          txApprover.approve = true;

          ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          rids.put(initialAmountOfRecords + i, document.getIdentity());
          values.put(document.getIdentity(), value);

          txApprover.approve = false;

          session.begin();
          try {
            Set<ORID> ridsToDelete = new HashSet<>();
            while (ridsToDelete.size() < 10) {
              Integer ridToDelete = rids.ceilingKey(random.nextInt(rids.lastKey()));
              if (ridToDelete == null) {
                ridToDelete = rids.firstKey();
              }

              ridsToDelete.add(rids.get(ridToDelete));
            }

            for (ORID orid : ridsToDelete) {
              final ODocument doc = session.load(orid);
              Assert.assertNotNull(doc);
              doc.delete();
            }

            session.commit();
          } catch (NotApprovedException e) {
            //continue
          }

          txApprover.approve = true;

          Integer ridToDelete = rids.ceilingKey(random.nextInt(rids.lastKey()));
          if (ridToDelete == null) {
            ridToDelete = rids.firstKey();
          }
          document = session.load(rids.get(ridToDelete));
          Assert.assertNotNull(document);
          document.delete();

          rids.remove(ridToDelete);
          values.remove(document.getIdentity());

          Integer ridToUpdate = rids.ceilingKey(random.nextInt(rids.lastKey()));
          if (ridToUpdate == null) {
            ridToUpdate = rids.firstKey();
          }

          final int valLen = random.nextInt(200) + 100;
          final byte[] val = new byte[valLen];
          random.nextBytes(val);
          ensureValueIsUnique(random, val);

          document = session.load(rids.get(ridToUpdate));
          document.field("value", val);
          document.save();
          values.put(document.getIdentity(), val);

          if (i > 0 && i % 10_000 == 0) {
            System.out.printf("%d iterations are tested out of %d%n", i, testedAmountOfRecords);
          }
        }

        session.close();
        session = orient.open(DB_NAME, "admin", "admin");

        txApprover.approve = true;
        System.out.println("Testing all loaded records");
        OIndex index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        System.out.println("Loading additional set of records");
        for (int i = 0; i < additionalAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, additionalAmountOfRecords);
          }
        }

        session.close();
        session = orient.open(DB_NAME, "admin", "admin");

        System.out.println("Testing all loaded records");
        index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords + additionalAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        if (k < iterationsCount - 1) {
          dropSchema();
          createSchema();
        }
      } catch (Exception | Error e) {
        System.out.printf("testDeletionRollbackThree seed: %d%n", seed);
        throw e;
      }
    }
  }

  @Test
  public void testRollbackMixOne() {
    long seed = -1;
    for (int k = 0; k < iterationsCount; k++) {
      System.out.printf("Iteration %d out of %d%n", k + 1, iterationsCount);
      try {
        seed = System.nanoTime();
        Random random = new Random(seed);

        TreeMap<Integer, ORID> rids = new TreeMap<>();
        Map<ORID, byte[]> values = new HashMap<>();

        System.out.println("Loading initial set of records");
        for (int i = 0; i < initialAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          rids.put(i, document.getIdentity());
          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, initialAmountOfRecords);
          }
        }

        System.out.println("Testing rollback of mix of operations");
        for (int i = 0; i < testedAmountOfRecords; i++) {
          txApprover.approve = true;

          ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          rids.put(initialAmountOfRecords + i, document.getIdentity());
          values.put(document.getIdentity(), value);

          txApprover.approve = false;

          session.begin();
          try {
            Set<ORID> ridsToDelete = new HashSet<>();
            while (ridsToDelete.size() < 3) {
              Integer ridToDelete = rids.ceilingKey(random.nextInt(rids.lastKey()));
              if (ridToDelete == null) {
                ridToDelete = rids.firstKey();
              }

              ridsToDelete.add(rids.get(ridToDelete));
            }

            for (ORID orid : ridsToDelete) {
              final ODocument doc = session.load(orid);
              Assert.assertNotNull(doc);
              doc.delete();
            }

            Set<ORID> ridsToUpdate = new HashSet<>();
            while (ridsToUpdate.size() < 3) {
              Integer ridToUpdate = rids.ceilingKey(random.nextInt(rids.lastKey()));
              if (ridToUpdate == null) {
                ridToUpdate = rids.firstKey();
              }

              if (!ridsToDelete.contains(rids.get(ridToUpdate))) {
                ridsToUpdate.add(rids.get(ridToUpdate));
              }
            }

            for (ORID orid : ridsToUpdate) {
              final ODocument doc = session.load(orid);
              final int valLen = random.nextInt(200) + 100;
              final byte[] val = new byte[valLen];
              random.nextBytes(val);
              ensureValueIsUnique(random, val);

              doc.field("value", val);
              doc.save();
            }

            for (int n = 0; n < 3; n++) {
              final ODocument doc = new ODocument(CLASS_NAME);

              final int valLen = random.nextInt(200) + 100;
              final byte[] val = new byte[valLen];
              random.nextBytes(val);
              ensureValueIsUnique(random, val);

              doc.field("value", val);
              doc.save();
            }
            session.commit();
          } catch (NotApprovedException e) {
            //continue
          }

          txApprover.approve = true;

          Integer ridToDelete = rids.ceilingKey(random.nextInt(rids.lastKey()));
          if (ridToDelete == null) {
            ridToDelete = rids.firstKey();
          }
          document = session.load(rids.get(ridToDelete));
          Assert.assertNotNull(document);
          document.delete();

          rids.remove(ridToDelete);
          values.remove(document.getIdentity());

          Integer ridToUpdate = rids.ceilingKey(random.nextInt(rids.lastKey()));
          if (ridToUpdate == null) {
            ridToUpdate = rids.firstKey();
          }

          final int valLen = random.nextInt(200) + 100;
          final byte[] val = new byte[valLen];
          random.nextBytes(val);
          ensureValueIsUnique(random, val);

          document = session.load(rids.get(ridToUpdate));
          document.field("value", val);
          document.save();
          values.put(document.getIdentity(), val);

          if (i > 0 && i % 10_000 == 0) {
            System.out.printf("%d iterations are tested out of %d%n", i, testedAmountOfRecords);
          }
        }

        session.close();
        session = orient.open(DB_NAME, "admin", "admin");

        txApprover.approve = true;
        System.out.println("Testing all loaded records");
        OIndex index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        System.out.println("Loading additional set of records");
        for (int i = 0; i < additionalAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, additionalAmountOfRecords);
          }
        }

        session.close();
        session = orient.open(DB_NAME, "admin", "admin");

        System.out.println("Testing all loaded records");
        index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords + additionalAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        if (k < iterationsCount - 1) {
          dropSchema();
          createSchema();
        }
      } catch (Exception | Error e) {
        System.out.printf("testRollbackMixOne seed: %d%n", seed);
        throw e;
      }
    }
  }

  @Test
  public void testRollbackMixTwo() {
    long seed = -1;
    for (int k = 0; k < iterationsCount; k++) {
      System.out.printf("Iteration %d out of %d%n", k + 1, iterationsCount);
      try {
        seed = System.nanoTime();
        Random random = new Random(seed);

        TreeMap<Integer, ORID> rids = new TreeMap<>();
        Map<ORID, byte[]> values = new HashMap<>();

        System.out.println("Loading initial set of records");
        for (int i = 0; i < initialAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          rids.put(i, document.getIdentity());
          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, initialAmountOfRecords);
          }
        }

        System.out.println("Testing rollback of mix of operations");
        for (int i = 0; i < testedAmountOfRecords; i++) {
          txApprover.approve = true;

          ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          rids.put(initialAmountOfRecords + i, document.getIdentity());
          values.put(document.getIdentity(), value);

          txApprover.approve = false;

          session.begin();
          try {
            Set<ORID> ridsToUpdate = new HashSet<>();
            while (ridsToUpdate.size() < 3) {
              Integer ridToUpdate = rids.ceilingKey(random.nextInt(rids.lastKey()));
              if (ridToUpdate == null) {
                ridToUpdate = rids.firstKey();
              }

              ridsToUpdate.add(rids.get(ridToUpdate));
            }

            for (ORID orid : ridsToUpdate) {
              final ODocument doc = session.load(orid);
              final int valLen = random.nextInt(200) + 100;
              final byte[] val = new byte[valLen];
              random.nextBytes(val);
              ensureValueIsUnique(random, val);

              doc.field("value", val);
              doc.save();
            }

            Set<ORID> ridsToDelete = new HashSet<>();
            while (ridsToDelete.size() < 3) {
              Integer ridToDelete = rids.ceilingKey(random.nextInt(rids.lastKey()));
              if (ridToDelete == null) {
                ridToDelete = rids.firstKey();
              }

              ridsToDelete.add(rids.get(ridToDelete));
            }

            for (ORID orid : ridsToDelete) {
              final ODocument doc = session.load(orid);
              Assert.assertNotNull(doc);
              doc.delete();
            }

            for (int n = 0; n < 3; n++) {
              final ODocument doc = new ODocument(CLASS_NAME);

              final int valLen = random.nextInt(200) + 100;
              final byte[] val = new byte[valLen];
              random.nextBytes(val);
              ensureValueIsUnique(random, val);

              doc.field("value", val);
              doc.save();
            }
            session.commit();
          } catch (NotApprovedException e) {
            //continue
          }

          txApprover.approve = true;

          Integer ridToDelete = rids.ceilingKey(random.nextInt(rids.lastKey()));
          if (ridToDelete == null) {
            ridToDelete = rids.firstKey();
          }
          document = session.load(rids.get(ridToDelete));
          Assert.assertNotNull(document);
          document.delete();

          rids.remove(ridToDelete);
          values.remove(document.getIdentity());

          Integer ridToUpdate = rids.ceilingKey(random.nextInt(rids.lastKey()));
          if (ridToUpdate == null) {
            ridToUpdate = rids.firstKey();
          }

          final int valLen = random.nextInt(200) + 100;
          final byte[] val = new byte[valLen];
          random.nextBytes(val);
          ensureValueIsUnique(random, val);

          document = session.load(rids.get(ridToUpdate));
          document.field("value", val);
          document.save();
          values.put(document.getIdentity(), val);

          if (i > 0 && i % 10_000 == 0) {
            System.out.printf("%d iterations are tested out of %d%n", i, testedAmountOfRecords);
          }
        }

        session.close();
        session = orient.open("TestDB", "admin", "admin");

        txApprover.approve = true;
        System.out.println("Testing all loaded records");
        OIndex index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        System.out.println("Loading additional set of records");
        for (int i = 0; i < additionalAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, additionalAmountOfRecords);
          }
        }

        session.close();
        session = orient.open(DB_NAME, "admin", "admin");

        System.out.println("Testing all loaded records");
        index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords + additionalAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        if (k < iterationsCount - 1) {
          dropSchema();
          createSchema();
        }
      } catch (Exception | Error e) {
        System.out.printf("testRollbackMixTwo seed : %d%n", seed);
        throw e;
      }
    }
  }

  @Test
  public void testRollbackMixThree() {
    long seed = -1;
    for (int k = 0; k < iterationsCount; k++) {
      System.out.printf("Iteration %d out of %d%n", k + 1, iterationsCount);
      try {
        seed = System.nanoTime();
        Random random = new Random(seed);

        TreeMap<Integer, ORID> rids = new TreeMap<>();
        Map<ORID, byte[]> values = new HashMap<>();

        System.out.println("Loading initial set of records");
        for (int i = 0; i < initialAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          rids.put(i, document.getIdentity());
          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, initialAmountOfRecords);
          }
        }

        System.out.println("Testing rollback of mix of operations");
        for (int i = 0; i < testedAmountOfRecords; i++) {
          txApprover.approve = true;

          ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          rids.put(initialAmountOfRecords + i, document.getIdentity());
          values.put(document.getIdentity(), value);

          txApprover.approve = false;

          session.begin();
          try {
            for (int n = 0; n < 3; n++) {
              final ODocument doc = new ODocument(CLASS_NAME);

              final int valLen = random.nextInt(200) + 100;
              final byte[] val = new byte[valLen];
              random.nextBytes(val);
              ensureValueIsUnique(random, val);

              doc.field("value", val);
              doc.save();
            }

            Set<ORID> ridsToUpdate = new HashSet<>();
            while (ridsToUpdate.size() < 3) {
              Integer ridToUpdate = rids.ceilingKey(random.nextInt(rids.lastKey()));
              if (ridToUpdate == null) {
                ridToUpdate = rids.firstKey();
              }

              ridsToUpdate.add(rids.get(ridToUpdate));
            }

            for (ORID orid : ridsToUpdate) {
              final ODocument doc = session.load(orid);
              final int valLen = random.nextInt(200) + 100;
              final byte[] val = new byte[valLen];
              random.nextBytes(val);
              ensureValueIsUnique(random, val);

              doc.field("value", val);
              doc.save();
            }

            Set<ORID> ridsToDelete = new HashSet<>();
            while (ridsToDelete.size() < 3) {
              Integer ridToDelete = rids.ceilingKey(random.nextInt(rids.lastKey()));
              if (ridToDelete == null) {
                ridToDelete = rids.firstKey();
              }

              ridsToDelete.add(rids.get(ridToDelete));
            }

            for (ORID orid : ridsToDelete) {
              final ODocument doc = session.load(orid);
              Assert.assertNotNull(doc);
              doc.delete();
            }

            session.commit();
          } catch (NotApprovedException e) {
            //continue
          }

          txApprover.approve = true;

          Integer ridToDelete = rids.ceilingKey(random.nextInt(rids.lastKey()));
          if (ridToDelete == null) {
            ridToDelete = rids.firstKey();
          }
          document = session.load(rids.get(ridToDelete));
          Assert.assertNotNull(document);
          document.delete();

          rids.remove(ridToDelete);
          values.remove(document.getIdentity());

          Integer ridToUpdate = rids.ceilingKey(random.nextInt(rids.lastKey()));
          if (ridToUpdate == null) {
            ridToUpdate = rids.firstKey();
          }

          final int valLen = random.nextInt(200) + 100;
          final byte[] val = new byte[valLen];
          random.nextBytes(val);
          ensureValueIsUnique(random, val);

          document = session.load(rids.get(ridToUpdate));
          document.field("value", val);
          document.save();
          values.put(document.getIdentity(), val);

          if (i > 0 && i % 10_000 == 0) {
            System.out.printf("%d iterations are tested out of %d%n", i, testedAmountOfRecords);
          }
        }

        session.close();
        session = orient.open(DB_NAME, "admin", "admin");

        txApprover.approve = true;
        System.out.println("Testing all loaded records");
        OIndex index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        System.out.println("Loading additional set of records");
        for (int i = 0; i < additionalAmountOfRecords; i++) {
          final ODocument document = new ODocument(CLASS_NAME);
          final int recordSize = random.nextInt(200) + 100;
          final byte[] value = new byte[recordSize];
          random.nextBytes(value);
          ensureValueIsUnique(random, value);

          document.field("value", value);
          document.save();

          values.put(document.getIdentity(), value);

          if (i > 0 && i % 20_000 == 0) {
            System.out.printf("%d records are loaded out of %d%n", i, additionalAmountOfRecords);
          }
        }

        session.close();
        session = orient.open(DB_NAME, "admin", "admin");

        System.out.println("Testing all loaded records");
        index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
        Assert.assertEquals(initialAmountOfRecords + additionalAmountOfRecords, index.getSize());
        iterateOverAllRecords(values);

        if (k < iterationsCount - 1) {
          dropSchema();
          createSchema();
        }

      } catch (Exception | Error e) {
        System.out.printf("testRollbackMixThree seed : %d%n", seed);
        throw e;
      }
    }
  }


  private static void iterateOverAllRecords(Map<ORID, byte[]> values) {
    OIndex index = session.getMetadata().getIndexManager().getIndex(VALUE_INDEX);
    OIndexCursor cursor = index.cursor();

    Map.Entry<Object, OIdentifiable> entry = cursor.nextEntry();
    while (entry != null) {
      ORID rid = entry.getValue().getIdentity();
      byte[] value = (byte[]) entry.getKey();

      Assert.assertArrayEquals(values.get(rid), value);
      entry = cursor.nextEntry();
    }
  }

  private static void ensureValueIsUnique(Random random, byte[] value) {
    while (true) {
      try (OResultSet resultSet = session.query("select count(*) from " + CLASS_NAME + " where value = ?", (Object) value)) {
        final OResult result = resultSet.next();
        if (result.<Long>getProperty("count(*)") > 0) {
          random.nextBytes(value);
        } else {
          break;
        }
      }
    }
  }

  private static final class TestTXApprover implements OTXApprover {
    private boolean approve = true;

    @Override
    public void approve() {
      if (!approve) {
        throw new NotApprovedException();
      }
    }
  }

  private static final class NotApprovedException extends OException implements OHighLevelException {
    NotApprovedException() {
      super("TX is not approved");
    }
  }
}
