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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

/** MyTest. */
public class MyTest {

    private Path databasePath;
    private LocalFileIO fileIO;

    @BeforeEach
    void setUp() {
        databasePath = new Path("./warehouse/default");
        fileIO = new LocalFileIO();
    }

    @Test
    void testPaimonSchema() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("k1", DataTypes.INT())
                        .column("k2", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .primaryKey("k1", "k2")
                        //                        .option("target-file-size", "16B")
                        .build();

        String tableName = "t1";
        dropTableIfExist(tableName);
        TableSchema tableSchema = getSchemaManager(tableName).createTable(schema, true);

        FileStoreTable table =
                FileStoreTableFactory.create(fileIO, getTablePath(tableName), tableSchema);

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();

        try (StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commiter = streamWriteBuilder.newCommit()) {
            //            for (int commit = 0; commit < 5; ++commit) {
            for (int i = 0; i < 1000; ++i) {
                write.write(
                        GenericRow.of(
                                i % 2000, i % 2000, BinaryString.fromString(String.valueOf(i))),
                        0);
            }
            commiter.commit(0, write.prepareCommit(true, 0));

            //            }
        }
    }

    @Test
    void testPaimonPartialUpdate() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v1", DataTypes.STRING())
                        .column("s1", DataTypes.INT())
                        .column("v2", DataTypes.STRING())
                        .column("s2", DataTypes.INT())
                        .primaryKey("k")
                        .option("sequence.field", "s1")
                        //                        .option("fields.s1.sequence-group", "v1")
                        //                        .option("fields.s2.sequence-group", "v2")
                        //                        .option("merge-engine", "partial-update")
                        .option("num-sorted-run.compaction-trigger", "3")
                        .build();

        String tableName = "partial_t";
        dropTableIfExist(tableName);
        TableSchema tableSchema = getSchemaManager(tableName).createTable(schema, true);

        FileStoreTable table =
                FileStoreTableFactory.create(fileIO, getTablePath(tableName), tableSchema);

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();

        try (StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commiter = streamWriteBuilder.newCommit()) {
            for (int i = 0; i < 10000; ++i) {
                write.write(
                        GenericRow.of(i % 2000, BinaryString.fromString(i + "10"), 0, null, null),
                        0);
            }
            commiter.commit(0, write.prepareCommit(true, 0));
        }

        try (StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commiter = streamWriteBuilder.newCommit()) {
            for (int i = 0; i < 10000; ++i) {
                write.write(
                        GenericRow.of(i % 2000, null, null, BinaryString.fromString(i + "20"), 0),
                        0);
            }
            commiter.commit(1, write.prepareCommit(true, 1));
        }

        try (StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commiter = streamWriteBuilder.newCommit()) {
            for (int i = 0; i < 10000; ++i) {
                write.write(
                        GenericRow.of(i % 2000, BinaryString.fromString(i + "11"), 1, null, null),
                        0);
            }
            commiter.commit(2, write.prepareCommit(true, 2));
        }

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            RecordReader.RecordIterator<InternalRow> iter = reader.readBatch();

            while (iter != null) {
                InternalRow row = iter.next();
                if (row == null) {
                    iter.releaseBatch();
                    iter = reader.readBatch();
                    continue;
                }
                System.out.printf(
                        "%d, %s, %d, %s, %d\n",
                        row.getInt(0),
                        row.getString(1),
                        row.getInt(2),
                        row.getString(3),
                        row.getInt(4));
            }
        }
    }

    @Test
    void testPaimonChangelog() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .primaryKey("k")
                        .option("changelog-producer", "input")
                        .build();

        String tableName = "changelog_t";
        dropTableIfExist(tableName);
        TableSchema tableSchema = getSchemaManager(tableName).createTable(schema, true);

        FileStoreTable table =
                FileStoreTableFactory.create(fileIO, getTablePath(tableName), tableSchema);

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();

        try (StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commiter = streamWriteBuilder.newCommit()) {
            for (int i = 0; i < 10000; ++i) {
                write.write(
                        GenericRow.of(i % 2000, BinaryString.fromString(i + "10"), 0, null, null),
                        0);
            }

            for (int i = 0; i < 1000; ++i) {
                write.write(GenericRow.ofKind(RowKind.DELETE, i, null, null, null, null), 0);
            }
            commiter.commit(0, write.prepareCommit(true, 0));
        }

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            RecordReader.RecordIterator<InternalRow> iter = reader.readBatch();

            while (iter != null) {
                InternalRow row = iter.next();
                if (row == null) {
                    iter.releaseBatch();
                    iter = reader.readBatch();
                    continue;
                }
                System.out.printf("%d, %s\n", row.getInt(0), row.getString(1));
            }
        }
    }

    @Test
    void testKeepPaimonWrite() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .primaryKey("k")
                        .option("target-file-size", "16B")
                        .option("write-buffer-size", "256 kb")
                        .build();

        String tableName = "bigt1";
        dropTableIfExist(tableName);
        TableSchema tableSchema = getSchemaManager(tableName).createTable(schema, true);

        FileStoreTable table =
                FileStoreTableFactory.create(fileIO, getTablePath(tableName), tableSchema);

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();

        try (StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commiter = streamWriteBuilder.newCommit()) {
            //            for (int commit = 0; commit < 5; ++commit) {
            for (int i = 0; i < 20000; ++i) {
                write.write(GenericRow.of(i % 2000, BinaryString.fromString(String.valueOf(i))), 0);
            }
            commiter.commit(0, write.prepareCommit(true, 0));

            //            }
        }
    }

    @Test
    void testColumnGroup() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT(), null)
                        .column("v1", DataTypes.STRING(), null)
                        .column("v2", DataTypes.DOUBLE(), null)
                        .column("g1", DataTypes.BIGINT(), null)
                        .column("g2", DataTypes.BIGINT(), null)
                        .primaryKey("k")
                        .option("target-file-size", "16B")
                        .option("write-buffer-size", "256 kb")
                        .option("num-sorted-run.compaction-trigger", "10000")
                        .option("merge-engine", "partial-update")
                        .option("fields.g1.sequence-group", "v1")
                        .option("fields.g2.sequence-group", "v2")
                        .build();

        String tableName = "column_group";
        dropTableIfExist(tableName);
        TableSchema tableSchema = getSchemaManager(tableName).createTable(schema, true);

        FileStoreTable table =
                FileStoreTableFactory.create(fileIO, getTablePath(tableName), tableSchema);

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();

        try (StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commiter = streamWriteBuilder.newCommit()) {
            for (int i = 0; i < 20000; ++i) {
                write.write(
                        GenericRow.of(
                                i % 4000,
                                BinaryString.fromString(String.valueOf(i)),
                                i * 1.0,
                                (long) i,
                                (long) i),
                        i % 2);
            }
            commiter.commit(0, write.prepareCommit(true, 0));
            //            commiter.filterAndCommit(Collections.singletonMap(0L,
            // write.prepareCommit(true, 0)));
        }

        //        ReadBuilder readBuilder = table.newReadBuilder();
        //        TableScan.Plan plan = readBuilder.newScan().plan();
        //        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan))
        // {
        //            RecordReader.RecordIterator<InternalRow> iter = reader.readBatch();
        //
        //            while (iter != null) {
        //                InternalRow row = iter.next();
        //                if (row == null) {
        //                    iter.releaseBatch();
        //                    iter = reader.readBatch();
        //                    continue;
        //                }
        //                System.out.printf("%d, %s\n", row.getInt(0), row.getString(1));
        //            }
        //        }
    }

    @Test
    void testReadColumnGroup() throws Exception {
        String tableName = "column_group";
        SchemaManager schemaManager = getSchemaManager(tableName);
        FileStoreTable table =
                FileStoreTableFactory.create(
                        fileIO,
                        getTablePath(tableName),
                        schemaManager.latest().orElseThrow(RuntimeException::new));

        ReadBuilder readBuilder =
                table.newReadBuilder().withReadType(table.rowType().project("k", "v1", "v2"));
        TableScan.Plan batchPlan = readBuilder.newScan().plan();
        TableScan.Plan streamPlan = readBuilder.newStreamScan().plan();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(batchPlan)) {
            RecordReader.RecordIterator<InternalRow> iter = reader.readBatch();

            while (iter != null) {
                InternalRow row = iter.next();
                if (row == null) {
                    iter.releaseBatch();
                    iter = reader.readBatch();
                    continue;
                }
                System.out.printf(
                        "%d, %s, %f\n", row.getInt(0), row.getString(1), row.getDouble(2));
                //                System.out.printf("%d\n", row.getInt(0));
            }
        }
    }

    @Test
    void testColumnGroupCompaction() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT(), null)
                        .column("v1", DataTypes.STRING(), null)
                        .column("g1", DataTypes.BIGINT(), null)
                        .column("v2", DataTypes.DOUBLE(), null)
                        .column("g2", DataTypes.BIGINT(), null)
                        .primaryKey("k")
                        .option("num-sorted-run.compaction-trigger", "2")
                        .option("merge-engine", "partial-update")
                        .option("fields.g1.sequence-group", "v1")
                        .option("fields.g2.sequence-group", "v2")
                        .build();

        String tableName = "column_group_compaction";
        dropTableIfExist(tableName);
        TableSchema tableSchema = getSchemaManager(tableName).createTable(schema, true);

        FileStoreTable table =
                FileStoreTableFactory.create(fileIO, getTablePath(tableName), tableSchema);

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();

        try (StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commiter = streamWriteBuilder.newCommit()) {
            write.write(GenericRow.of(0, BinaryString.fromString("0"), 2L, 0.0, 0L), 0);
            write.write(GenericRow.of(1, BinaryString.fromString("1"), 0L, 1.1, null), 0);
            commiter.commit(0, write.prepareCommit(false, 0));
        }

        try (StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commiter = streamWriteBuilder.newCommit()) {
            write.write(GenericRow.of(0, BinaryString.fromString("00"), 1L, 1.0, 10L), 0);
            write.write(GenericRow.of(2, BinaryString.fromString("22"), 1L, 2.2, 20L), 0);
            commiter.commit(1, write.prepareCommit(true, 1));
        }

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            RecordReader.RecordIterator<InternalRow> iter = reader.readBatch();

            while (iter != null) {
                InternalRow row = iter.next();
                if (row == null) {
                    iter.releaseBatch();
                    iter = reader.readBatch();
                    continue;
                }
                System.out.printf(
                        "%d, %s, %d, %f, %d\n",
                        row.getInt(0),
                        row.getString(1),
                        row.getLong(2),
                        row.getDouble(3),
                        row.getLong(4));
                //                System.out.printf("%d\n", row.getInt(0));
            }
        }
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

    //    @Test
    //    void testReadPaimon() throws Exception {
    //
    //        TableSchema tableSchema =
    //                schemaManager.latest().orElseThrow(() -> new RuntimeException(""));
    //
    //        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath, tableSchema);
    //
    //        System.out.println(table);
    //
    //        ReadBuilder readBuilder = table.newReadBuilder();
    //
    //        TableScan.Plan plan = readBuilder.newScan().plan();
    //        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
    //
    //            KeyValueDataFileRecordReader kvReader =
    //                    new KeyValueDataFileRecordReader(
    //                            reader, RowType.of(DataTypes.INT()), table.rowType(), 0);
    //            RecordReader.RecordIterator<KeyValue> iter = kvReader.readBatch();
    //            while (iter != null) {
    //                KeyValue kv = iter.next();
    //                if (kv == null) {
    //                    iter.releaseBatch();
    //                    iter = kvReader.readBatch();
    //                    continue;
    //                }
    //                System.out.printf("%d, %s\n", kv.value().getInt(0), kv.value().getString(1));
    //            }
    //        }
    //    }
}
