/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.datagen.RandomGenerator;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.StringJoiner;

/** Stress test for column group compaction. */
public class ColumnGroupStressTest {

    private Path databasePath;
    private LocalFileIO fileIO;

    @TempDir private java.nio.file.Path temporaryFolder;

    @BeforeEach
    void setUp() {
        databasePath = new Path("./warehouse/default");
        fileIO = new LocalFileIO();
    }

    @Test
    void testColumnGroupCompactionStressDataGen() throws Exception {
        Schema schema = getSchema(4, 300, true, true);

        String tableName = "cg_compaction_stress";
        dropTableIfExist(tableName);
        TableSchema tableSchema = getSchemaManager(tableName).createTable(schema, true);

        FileStoreTable table =
                FileStoreTableFactory.create(fileIO, getTablePath(tableName), tableSchema);

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        RandomGenerator<String> stringGenerator = RandomGenerator.stringGenerator(100);
        stringGenerator.open();

        writeForColumnGroup(0, writeBuilder, stringGenerator);
        writeForColumnGroup(1, writeBuilder, stringGenerator);
        writeForColumnGroup(2, writeBuilder, stringGenerator);
        writeForColumnGroup(3, writeBuilder, stringGenerator);

        table.createTag("base");
    }

    @Test
    void testColumnGroupCompactionStressOverwriteToTest() throws Exception {
        Schema schema = getSchema(4, 300, true, true);

        String tableName = "cg_compaction_stress";
        TableSchema tableSchema = getSchemaManager(tableName).createTable(schema, true);

        FileStoreTable table =
                FileStoreTableFactory.create(fileIO, getTablePath(tableName), tableSchema);

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();

        writeSimpleForColumnGroup(0, writeBuilder);
        writeSimpleForColumnGroup(1, writeBuilder);
        writeSimpleForColumnGroup(2, writeBuilder);
        writeSimpleForColumnGroup(3, writeBuilder);

        table.createTag("base2");
    }

    @Test
    void testCompactionStressDataGen() throws Exception {
        Schema schema = getSchema(4, 300, true, false);

        String tableName = "compaction_stress";
        dropTableIfExist(tableName);
        TableSchema tableSchema = getSchemaManager(tableName).createTable(schema, true);

        FileStoreTable table =
                FileStoreTableFactory.create(fileIO, getTablePath(tableName), tableSchema);

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        RandomGenerator<String> stringGenerator = RandomGenerator.stringGenerator(100);
        stringGenerator.open();

        writeForColumnGroup(0, writeBuilder, stringGenerator);
        writeForColumnGroup(1, writeBuilder, stringGenerator);
        writeForColumnGroup(2, writeBuilder, stringGenerator);
        writeForColumnGroup(3, writeBuilder, stringGenerator);

        table.createTag("base");
    }

    @Test
    void testColumnGroupCompactionStressCompaction() throws Exception {

        String tableName = "cg_compaction_stress";
        TableSchema tableSchema = getSchemaManager(tableName).latest().get();

        FileStoreTable table =
                FileStoreTableFactory.create(fileIO, getTablePath(tableName), tableSchema);

        table.options().put("write-only", "false");
        table.rollbackTo("base");

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();

        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commiter = writeBuilder.newCommit()) {
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            commiter.commit(write.prepareCommit());
        }
    }

    @Test
    void testCompactionStressCompaction() throws Exception {

        String tableName = "compaction_stress";
        TableSchema tableSchema = getSchemaManager(tableName).latest().get();

        FileStoreTable table =
                FileStoreTableFactory.create(fileIO, getTablePath(tableName), tableSchema);

        table.options().put("write-only", "false");
        table.rollbackTo("base");

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();

        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commiter = writeBuilder.newCommit()) {
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            commiter.commit(write.prepareCommit());
        }
    }

    private void writeForColumnGroup(
            int groupIdx, BatchWriteBuilder writeBuilder, RandomGenerator<String> stringGenerator)
            throws Exception {

        int groupFieldOffset = 1 + groupIdx * (300 + 1);
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commiter = writeBuilder.newCommit()) {
            write.withIOManager(new IOManagerImpl(temporaryFolder.toString()));
            System.out.println("writing group idx: " + groupIdx);
            for (int key = 0; key < 100000; key++) {
                if (key % 1000 == 0) {
                    System.out.println("write key: " + key);
                }
                GenericRow row = new GenericRow(1205);
                row.setField(0, key);
                for (int i = groupFieldOffset; i < groupFieldOffset + 300; i++) {
                    row.setField(i, BinaryString.fromString(stringGenerator.next()));
                }
                row.setField(groupFieldOffset + 300, System.currentTimeMillis());

                write.write(row, 0);
            }

            commiter.commit(write.prepareCommit());
        }
    }

    private void writeSimpleForColumnGroup(int groupIdx, BatchWriteBuilder writeBuilder)
            throws Exception {

        int groupFieldOffset = 1 + groupIdx * (300 + 1);
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commiter = writeBuilder.newCommit()) {
            write.withIOManager(new IOManagerImpl(temporaryFolder.toString()));
            System.out.println("writing group idx: " + groupIdx);
            for (int key = 0; key < 100000; key++) {
                if (key % 1000 == 0) {
                    System.out.println("write key: " + key);
                }
                GenericRow row = new GenericRow(1205);
                row.setField(0, key);
                for (int i = groupFieldOffset; i < groupFieldOffset + 300; i++) {
                    row.setField(i, BinaryString.fromString(String.format("%d", key)));
                }
                row.setField(groupFieldOffset + 300, System.currentTimeMillis());

                write.write(row, 0);
            }

            commiter.commit(write.prepareCommit());
        }
    }

    private Schema getSchema(
            int numGroup, int colPerGroup, boolean writeOnly, boolean columnGroupEnabled) {
        Schema.Builder schemaBuilder = Schema.newBuilder();

        schemaBuilder.column("k", DataTypes.INT(), null).primaryKey("k");

        for (int i = 0; i < numGroup; i++) {
            int colOffset = i * colPerGroup;
            addColumnForGroup(schemaBuilder, i, colOffset, colPerGroup);
        }

        schemaBuilder.option("merge-engine", "partial-update");
        schemaBuilder.option("target-file-size", "2gb");
        //        schemaBuilder.option("write-buffer-size", "1gb");

        if (writeOnly) {
            schemaBuilder.option("write-only", "true");
        }

        if (columnGroupEnabled) {
            schemaBuilder.option("column-group.enabled", "true");
        }

        return schemaBuilder.build();
    }

    private static void addColumnForGroup(
            Schema.Builder schemaBuilder, int columnGroupId, int colOffset, int numCols) {

        StringJoiner joiner = new StringJoiner(",");
        for (int i = 0; i < numCols; i++) {
            schemaBuilder.column("f" + (colOffset + i), DataTypes.STRING(), null);
            joiner.add("f" + (colOffset + i));
        }

        String sequenceFieldName = "g" + columnGroupId;
        schemaBuilder.column(sequenceFieldName, DataTypes.BIGINT(), null);
        schemaBuilder.option("fields." + sequenceFieldName + ".sequence-group", joiner.toString());
    }

    private SchemaManager getSchemaManager(String tableName) {
        return new SchemaManager(fileIO, getTablePath(tableName));
    }

    private Path getTablePath(String tableName) {
        return new Path(databasePath, tableName);
    }

    private void dropTableIfExist(String tableName) throws IOException {
        Path tablePath = getTablePath(tableName);
        FileUtils.deleteDirectory(new File(tablePath.toString()));
    }
}
