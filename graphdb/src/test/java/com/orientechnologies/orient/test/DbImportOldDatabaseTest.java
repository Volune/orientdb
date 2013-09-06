/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.schedule.OScheduler;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Test
public class DbImportOldDatabaseTest implements OCommandOutputListener {
  public static final String EXPORT_FILE_PATH = "target/olddb.export.gz";
  public static final String NEW_DB_URL = "target/test-import-old";

  private String exportFile;
  private String importDbPath;

  @Parameters(value = {"testPath"})
  public DbImportOldDatabaseTest(String iTestPath) {
    exportFile = iTestPath + "/" + EXPORT_FILE_PATH;
    importDbPath = iTestPath + "/" + NEW_DB_URL;
    Orient.instance().getProfiler().startRecording();
  }

  @Test
  public void testDbExport() throws IOException {
    OGraphDatabase database = new OGraphDatabase("memory:exportolddb");
    if (database.exists()) {
      database.open("admin", "admin");
      database.drop();
    }
    database.create();

    OSchema schema = database.getMetadata().getSchema();
    schema.dropClass(OGraphDatabase.EDGE_ALIAS);
    schema.dropClass(OGraphDatabase.VERTEX_ALIAS);
    schema.dropClass(OMVRBTreeRIDProvider.PERSISTENT_CLASS_NAME);
    schema.dropClass(OScheduler.CLASSNAME); //old database schema doesn't have OSchedule class

    //rebuild graph classes
    database.checkForGraphSchema();

    schema.createClass("TestVertex", database.getVertexBaseClass());
    schema.createClass("TestEdge", database.getEdgeBaseClass());

    ODocument vertex1 = database.command(new OCommandSQL("create vertex V")).execute();
    ODocument vertex2 = database.command(new OCommandSQL("create vertex V")).execute();
    database.command(new OCommandSQL("create edge E"
        + " from " + vertex1.getIdentity()
        + " to " + vertex2.getIdentity()
        + " set someValue = 1 ")).execute(); //set a value to avoid lightweight edge

    ODocument testvertex1 = database.command(new OCommandSQL("create vertex TestVertex")).execute();
    ODocument testvertex2 = database.command(new OCommandSQL("create vertex TestVertex")).execute();
    database.command(new OCommandSQL("create edge TestEdge"
        + " from " + testvertex1.getIdentity()
        + " to " + testvertex2.getIdentity()
        + " set someValue = 1 ")).execute(); //set a value to avoid lightweight edge

    checkDatabase(database);

    ODatabaseExport export = new ODatabaseExport(database, exportFile, this);
    export.exportDatabase();
    export.close();

    database.close();
  }

  @Test(dependsOnMethods = "testDbExport")
  public void testDbImport() throws IOException {
    final File importDir = new File(importDbPath);
    if (importDir.exists())
      for (File f : importDir.listFiles())
        f.delete();
    else
      importDir.mkdir();

    OGraphDatabase database = new OGraphDatabase("local:" + importDbPath);
    database.create();

    database.getMetadata().getSchema().dropClass("ORIDs");
    database.getMetadata().getSchema().dropClass("V");
    database.getMetadata().getSchema().dropClass("E");

    ODatabaseImport impor = new ODatabaseImport(database, exportFile, this);

    // UNREGISTER ALL THE HOOKS
    for (ORecordHook hook : new ArrayList<ORecordHook>(database.getHooks())) {
      database.unregisterHook(hook);
    }

    impor.setDeleteRIDMapping(false);
    impor.importDatabase();
    impor.close();

    checkDatabase(database);

    database.close();
  }

  private void checkDatabase(OGraphDatabase database) {
    List vertexes = database.command(new OCommandSQL("select from V")).execute();
    Assert.assertEquals(4, vertexes.size());
    List edges = database.command(new OCommandSQL("select from E")).execute();
    Assert.assertEquals(2, edges.size());
    List testvertexes = database.command(new OCommandSQL("select from TestVertex")).execute();
    Assert.assertEquals(2, testvertexes.size());
    List testedges = database.command(new OCommandSQL("select from TestEdge")).execute();
    Assert.assertEquals(1, testedges.size());
  }

  @Test(enabled = false)
  public void onMessage(final String iText) {
    System.out.print(iText);
    System.out.flush();
  }
}
