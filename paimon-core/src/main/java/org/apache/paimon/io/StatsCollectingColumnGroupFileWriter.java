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

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueColumnGroupSerializer;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.apache.paimon.io.DataFilePathFactory.dataFileToFileIndexPath;

public class StatsCollectingColumnGroupFileWriter
        implements SingleFileWriter<KeyValue, DataFileMeta> {

    private AbstractSingleFileWriter<KeyValue, ?>[] writers;

    @Nullable private final SimpleStatsExtractor simpleStatsExtractor;
    private final FileIO fileIO;
    private final ColumnGroupWriterContext columnGroupWriterContext;
    private final Path path;
    private final TableSchema schema;
    private final String compression;
    private final boolean asyncWrite;
    private final RowType keyType;
    private final RowType valueType;
    private final RecordConverter<KeyValue> recordConverter;
    private final int level;
    private final FileSource fileSource;
    @Nullable private SimpleStatsCollector simpleStatsCollector = null;

    private final SimpleStatsConverter keyStatsConverter;
    private final SimpleStatsConverter valueStatsConverter;
    private final InternalRowSerializer keySerializer;
    @Nullable private final DataFileIndexWriter dataFileIndexWriter;

    private long recordCount;
    private boolean closed;

    private BinaryRow minKey = null;
    private InternalRow maxKey = null;
    private long minSeqNumber = Long.MAX_VALUE;
    private long maxSeqNumber = Long.MIN_VALUE;
    private long deleteRecordCount = 0;

    public StatsCollectingColumnGroupFileWriter(
            ColumnGroupWriterContext columnGroupWriterContext,
            Path path,
            TableSchema schema,
            String compression,
            boolean asyncWrite,
            RowType keyType,
            RowType valueType,
            RecordConverter<KeyValue> recordConverter,
            RowType writeSchema,
            @Nullable SimpleStatsExtractor simpleStatsExtractor,
            SimpleColStatsCollector.Factory[] statsCollectors,
            FileIO fileIO,
            CoreOptions options,
            FileIndexOptions fileIndexOptions,
            int level,
            FileSource fileSource) {
        this.columnGroupWriterContext = columnGroupWriterContext;
        this.path = path;
        this.schema = schema;
        this.compression = compression;
        this.asyncWrite = asyncWrite;
        this.keyType = keyType;
        this.valueType = valueType;

        this.recordConverter = recordConverter;
        this.level = level;
        this.fileSource = fileSource;
        this.fileIO = fileIO;

        this.recordCount = 0;
        this.closed = false;

        this.simpleStatsExtractor = simpleStatsExtractor;
        if (this.simpleStatsExtractor == null) {
            this.simpleStatsCollector = new SimpleStatsCollector(writeSchema, statsCollectors);
        }

        this.keySerializer = new InternalRowSerializer(keyType);

        this.keyStatsConverter = new SimpleStatsConverter(keyType);
        this.valueStatsConverter = new SimpleStatsConverter(valueType, options.statsDenseStore());
        this.dataFileIndexWriter =
                DataFileIndexWriter.create(
                        fileIO, dataFileToFileIndexPath(path), valueType, fileIndexOptions);

        this.writers = getColumnGroupWriters();
    }

    private ColumnGroupKeyValueDataFileWriter[] getColumnGroupWriters() {
        int numColumnGroup = getNumColumnGroup(schema);
        ColumnGroupKeyValueDataFileWriter[] writers =
                new ColumnGroupKeyValueDataFileWriter[numColumnGroup];

        for (int i = 0; i < numColumnGroup; i++) {
            writers[i] =
                    new ColumnGroupKeyValueDataFileWriter(
                            fileIO,
                            columnGroupWriterContext.getFormatWriterFactory(i),
                            toColumnGroupPath(i, path),
                            converterForColumnGroup(i),
                            compression,
                            asyncWrite);
        }

        return writers;
    }

    private Function<KeyValue, InternalRow> converterForColumnGroup(int i) {
        KeyValueColumnGroupSerializer serializer =
                new KeyValueColumnGroupSerializer(i, keyType, valueType);
        return (kv) -> serializer.toRow(kv.key(), kv.sequenceNumber(), kv.valueKind(), kv.value());
    }

    private Path toColumnGroupPath(int columnGroupId, Path path) {
        String[] split = path.getName().split("\\.");
        Preconditions.checkState(split.length == 2);
        String fileName = split[0];
        String format = split[1];
        return new Path(
                path.getParent(), String.format("%s-%d.%s", fileName, columnGroupId, format));
    }

    @Override
    public void write(KeyValue record) throws IOException {
        for (AbstractSingleFileWriter<KeyValue, ?> writer : writers) {
            writer.write(record);
        }
        recordCount++;

        if (dataFileIndexWriter != null) {
            dataFileIndexWriter.write(record.value());
        }

        updateMinKey(record);
        updateMaxKey(record);

        updateMinSeqNumber(record);
        updateMaxSeqNumber(record);

        if (record.valueKind().isRetract()) {
            deleteRecordCount++;
        }

        if (simpleStatsCollector != null && !simpleStatsCollector.isDisabled()) {
            simpleStatsCollector.collect(recordConverter.convert(record));
        }
    }

    public void writeBundle(BundleRecords bundle) throws IOException {
        Preconditions.checkState(
                simpleStatsExtractor != null,
                "Can't write bundle without simpleStatsExtractor, we may lose all the statistical information");

        for (AbstractSingleFileWriter<KeyValue, ?> writer : writers) {
            writer.writeBundle(bundle);
        }
        recordCount += bundle.rowCount();
    }

    @Override
    public long recordCount() {
        return recordCount;
    }

    @Override
    public void abort() {
        for (AbstractSingleFileWriter<KeyValue, ?> writer : writers) {
            writer.abort();
        }
    }

    @Override
    public DataFileMeta result() throws IOException {
        if (recordCount() == 0) {
            return null;
        }

        SimpleColStats[] rowStats = fieldStats();
        int numKeyFields = keyType.getFieldCount();

        SimpleColStats[] keyFieldStats = Arrays.copyOfRange(rowStats, 0, numKeyFields);
        SimpleStats keyStats = keyStatsConverter.toBinaryAllMode(keyFieldStats);

        SimpleColStats[] valFieldStats =
                Arrays.copyOfRange(rowStats, numKeyFields, rowStats.length);

        Pair<List<String>, SimpleStats> valueStatsPair =
                valueStatsConverter.toBinary(valFieldStats);

        DataFileIndexWriter.FileIndexResult indexResult =
                dataFileIndexWriter == null
                        ? DataFileIndexWriter.EMPTY_RESULT
                        : dataFileIndexWriter.result();

        return new DataFileMeta(
                path.getName(),
                0,
                recordCount,
                minKey,
                keySerializer.toBinaryRow(maxKey),
                keyStats,
                valueStatsPair.getValue(),
                minSeqNumber,
                maxSeqNumber,
                schema.id(),
                level,
                deleteRecordCount,
                indexResult.embeddedIndexBytes(),
                fileSource,
                valueStatsPair.getKey());
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            for (AbstractSingleFileWriter<KeyValue, ?> writer : writers) {
                writer.close();
            }
        } catch (IOException e) {
            abort();
            throw e;
        } finally {
            closed = true;
        }
    }

    public SimpleColStats[] fieldStats() throws IOException {
        Preconditions.checkState(closed, "Cannot access metric unless the writer is closed.");

        if (simpleStatsExtractor != null) {
            List<SimpleColStats[]> colStatsPerColumnGroup = new ArrayList<>();
            for (int i = 0; i < writers.length; i++) {
                SimpleStatsExtractor statsExtractor = columnGroupWriterContext.getStatsExtractor(i);
                colStatsPerColumnGroup.add(statsExtractor.extract(fileIO, writers[i].path));
            }

            int numCol = keyType.getFields().size() + valueType.getFields().size();
            SimpleColStats[] colStats = new SimpleColStats[numCol];
            int[] columnGroupIdx = new int[getNumColumnGroup(schema)];
            Arrays.fill(columnGroupIdx, 0);
            int currentIdx = 0;
            for (DataField field : keyType.getFields()) {
                int columnGroupId = field.getColumnGroupId();
                colStats[currentIdx++] =
                        colStatsPerColumnGroup.get(columnGroupId)[columnGroupIdx[columnGroupId]++];
            }

            for (DataField field : valueType.getFields()) {
                int columnGroupId = field.getColumnGroupId();
                colStats[currentIdx++] =
                        colStatsPerColumnGroup.get(columnGroupId)[columnGroupIdx[columnGroupId]++];
            }

            return colStats;
        } else {
            return simpleStatsCollector.extract();
        }
    }

    public boolean reachTargetSize(boolean suggestedCheck, long targetFileSize) throws IOException {
        return suggestedCheck && getTotalDataSize() > targetFileSize;
    }

    @Override
    public Path path() {
        return path;
    }

    private long getTotalDataSize() throws IOException {
        long totalSize = 0;
        for (AbstractSingleFileWriter<KeyValue, ?> writer : writers) {
            totalSize += writer.getEstimatedDataSize();
        }
        return totalSize;
    }

    public AbortExecutor abortExecutor() {
        List<AbstractSingleFileWriter.AbortExecutor> abortExecutors = new ArrayList<>();
        for (AbstractSingleFileWriter<KeyValue, ?> writer : writers) {
            abortExecutors.add(writer.abortExecutor());
        }
        return new AbortExecutor(
                abortExecutors.toArray(new AbstractSingleFileWriter.AbortExecutor[0]));
    }

    public interface RecordConverter<RECORD_T> {
        InternalRow convert(RECORD_T record);
    }

    static boolean isColumnGrouped(TableSchema schema) {
        return schema.fields().stream().allMatch(f -> f.getColumnGroupId() != -1);
    }

    private static int getNumColumnGroup(TableSchema schema) {
        return schema.fields().stream()
                        .map(DataField::getColumnGroupId)
                        .max(Integer::compareTo)
                        .orElse(-1)
                + 1;
    }

    /** Abort executor to just have reference of path instead of whole writer. */
    public static class AbortExecutor implements SingleFileWriter.AbortExecutor {
        private final AbstractSingleFileWriter.AbortExecutor[] abortExecutors;

        private AbortExecutor(AbstractSingleFileWriter.AbortExecutor[] abortExecutors) {
            this.abortExecutors = abortExecutors;
        }

        public void abort() {
            for (AbstractSingleFileWriter.AbortExecutor abortExecutor : abortExecutors) {
                abortExecutor.abort();
            }
        }
    }

    private void updateMinKey(KeyValue kv) {
        if (minKey == null) {
            minKey = keySerializer.toBinaryRow(kv.key()).copy();
        }
    }

    private void updateMaxKey(KeyValue kv) {
        maxKey = kv.key();
    }

    private void updateMinSeqNumber(KeyValue kv) {
        minSeqNumber = Math.min(minSeqNumber, kv.sequenceNumber());
    }

    private void updateMaxSeqNumber(KeyValue kv) {
        maxSeqNumber = Math.max(maxSeqNumber, kv.sequenceNumber());
    }

    public interface ColumnGroupWriterContext {
        FormatWriterFactory getFormatWriterFactory(int columnGroupId);

        SimpleStatsExtractor getStatsExtractor(int columnGroupId);
    }
}
