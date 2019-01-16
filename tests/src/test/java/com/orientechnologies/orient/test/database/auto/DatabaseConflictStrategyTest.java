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

package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public final class DatabaseConflictStrategyTest {

  private String             dbName;
  private OrientGraphFactory graphReadFactory;

  private DatabaseConflictStrategyTest() {
    this.dbName = "DatabaseConflictStrategyTest";
  }

  public static void main(String[] args) {
    DatabaseConflictStrategyTest test = new DatabaseConflictStrategyTest();
    test.runTest();
    Runtime.getRuntime().halt(0);
  }

  private void runTest() {
    OrientBaseGraph orientGraph = new OrientGraphNoTx(getDBURL());
    orientGraph.command(new OCommandSQL("ALTER database CONFLICTSTRATEGY 'automerge'")).execute();
    createVertexType(orientGraph);
    orientGraph.shutdown();

    OrientBaseGraph graph = getGraphFactory().getTx();

    Vertex vertex = graph.addVertex("class:Test");
    vertex.setProperty("prop1", "v1-1");
    vertex.setProperty("prop2", "v2-1");
    vertex.setProperty("prop3", "v3-1");
    graph.shutdown();

    Thread th1 = startThread(2, 1000);
    Thread th2 = startThread(3, 2000);
    Thread th3 = startThread(4, 3000);
    try {
      th1.join();
      th2.join();
      th3.join();
    } catch (Exception ex) {
      ex.printStackTrace();
    }

  }

  private String getDBURL() {
    return "memory:" + dbName;
  }

  private Thread startThread(final int version, final long timeout) {

    Thread th = new Thread(() -> {
      OrientVertex vtx1 = null;
      OrientGraph graph = getGraphFactory().getTx();
      Iterable<Vertex> vtxs = graph.getVertices();
      for (Vertex vtx : vtxs) {
        vtx1 = (OrientVertex) vtx;
      }
      try {
        Thread.sleep(timeout);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      vtx1.setProperty("prop1", "key-" + version);
      graph.commit();
      graph.shutdown();
    });
    th.start();
    return th;
  }

  private OrientGraphFactory getGraphFactory() {
    if (graphReadFactory == null) {
      graphReadFactory = new OrientGraphFactory(getDBURL()).setupPool(1, 10);
    }
    return graphReadFactory;
  }

  private static void createVertexType(OrientBaseGraph orientGraph) {
    OClass clazz = orientGraph.getVertexType("Test");
    if (clazz == null) {
      orientGraph.createVertexType("Test");
    }
  }

}
