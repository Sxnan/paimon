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

import org.apache.paimon.PartitionSettedRow;
import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.casting.CastedRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.ProjectedRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/** Reads {@link InternalRow} from data files. */
public class ColumnGroupDataFileRecordReader implements FileRecordReader<InternalRow> {

    private static final Logger LOG =
            LoggerFactory.getLogger(ColumnGroupDataFileRecordReader.class);

    private final Path path;
    private final int totalColumnGroup;
    private final Map<Integer, FileRecordReader<InternalRow>> readers;
    private final List<DataField> readDataField;
    @Nullable private final int[] indexMapping;
    @Nullable private final PartitionInfo partitionInfo;
    @Nullable private final CastFieldGetter[] castMapping;
    private final Map<Integer, FileRecordIterator<InternalRow>> columnGroupIters;
    private final FileRecordIterator<InternalRow> reusedIterator;

    public ColumnGroupDataFileRecordReader(
            ColumnGroupFileRecordReaderFactory columnGroupFileRecordReaderFactory,
            Path path,
            int totalColumnGroup,
            List<DataField> readDataFields,
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable PartitionInfo partitionInfo)
            throws IOException {
        this.path = path;
        this.totalColumnGroup = totalColumnGroup;
        this.readers = new HashMap<>();
        this.readDataField = readDataFields;

        for (int i = 0; i < totalColumnGroup; i++) {
            int columnGroupId = i;
            if (readDataFields.stream().anyMatch(f -> f.getColumnGroupId() == columnGroupId)) {
                this.readers.put(
                        columnGroupId,
                        columnGroupFileRecordReaderFactory.createReader(
                                i,
                                readDataFields.stream()
                                        .filter(f -> f.getColumnGroupId() == columnGroupId)
                                        .collect(Collectors.toList()),
                                toColumnGroupPath(i, path)));
            }
        }

        LOG.info("Read from the following column group readers {}", readers.keySet());
        this.indexMapping = indexMapping;
        this.partitionInfo = partitionInfo;
        this.castMapping = castMapping;
        columnGroupIters = new HashMap<>();
        reusedIterator =
                new ColumnGroupFileRecordIterator(
                        this.path, this.totalColumnGroup, columnGroupIters, readDataField);
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {

        for (Map.Entry<Integer, FileRecordReader<InternalRow>> entry : readers.entrySet()) {
            FileRecordIterator<InternalRow> iterator = entry.getValue().readBatch();
            if (iterator == null) {
                return null;
            }
            columnGroupIters.put(entry.getKey(), iterator);
        }

        FileRecordIterator<InternalRow> result = reusedIterator;
        // TODO: support mapping partition info and index mapping
        if (reusedIterator instanceof ColumnarRowIterator) {
            result = ((ColumnarRowIterator) reusedIterator).mapping(partitionInfo, indexMapping);
        } else {
            if (partitionInfo != null) {
                final PartitionSettedRow partitionSettedRow =
                        PartitionSettedRow.from(partitionInfo);
                result = reusedIterator.transform(partitionSettedRow::replaceRow);
            }
            if (indexMapping != null) {
                final ProjectedRow projectedRow = ProjectedRow.from(indexMapping);
                result = reusedIterator.transform(projectedRow::replaceRow);
            }
        }

        if (castMapping != null) {
            final CastedRow castedRow = CastedRow.from(castMapping);
            result = reusedIterator.transform(castedRow::replaceRow);
        }

        return result;
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<Integer, FileRecordReader<InternalRow>> entry : readers.entrySet()) {
            entry.getValue().close();
        }
    }

    private Path toColumnGroupPath(int columnGroupId, Path path) {
        String[] split = path.getName().split("\\.");
        Preconditions.checkState(split.length == 2);
        String fileName = split[0];
        String format = split[1];
        return new Path(
                path.getParent(), String.format("%s-%d.%s", fileName, columnGroupId, format));
    }

    /** Factory to create {@link FileRecordReader}. */
    public interface ColumnGroupFileRecordReaderFactory {
        FileRecordReader<InternalRow> createReader(
                int columnGroupId, List<DataField> readDataFields, Path path) throws IOException;
    }

    private static class ColumnGroupFileRecordIterator implements FileRecordIterator<InternalRow> {
        private final Path path;
        private final Map<Integer, FileRecordIterator<InternalRow>> columnGroupRecordIters;
        private final Map<Integer, InternalRow> columnGroupRows;
        private final ColumnGroupedRow reusedRow;

        ColumnGroupFileRecordIterator(
                Path path,
                int columnGroupNum,
                Map<Integer, FileRecordIterator<InternalRow>> columnGroupRecordIters,
                List<DataField> readDataFields) {
            this.path = path;
            this.columnGroupRecordIters = columnGroupRecordIters;
            this.columnGroupRows = new HashMap<>();
            this.reusedRow = new ColumnGroupedRow(columnGroupNum, columnGroupRows, readDataFields);
        }

        @Override
        public long returnedPosition() {
            return columnGroupRecordIters.get(0).returnedPosition();
        }

        @Override
        public Path filePath() {
            return path;
        }

        @Override
        public InternalRow next() throws IOException {
            //            Map<Integer, InternalRow> columnGroupRows = new HashMap<>();
            for (Map.Entry<Integer, FileRecordIterator<InternalRow>> entry :
                    columnGroupRecordIters.entrySet()) {
                InternalRow columnGroupRow = entry.getValue().next();
                if (columnGroupRow == null) {
                    // Any column group ended means all the column group ended.
                    return null;
                }
                columnGroupRows.put(entry.getKey(), columnGroupRow);
            }

            return reusedRow;
        }

        @Override
        public void releaseBatch() {
            for (Map.Entry<Integer, FileRecordIterator<InternalRow>> entry :
                    columnGroupRecordIters.entrySet()) {
                entry.getValue().releaseBatch();
            }
        }
    }

    private static class ColumnGroupedRow implements InternalRow {
        private final int columnGroupNum;
        private final Map<Integer, InternalRow> columnGroupRows;
        private final List<DataField> readDataFields;
        private final int[][] columnGroupRowMapping;

        public ColumnGroupedRow(
                int columnGroupNum,
                Map<Integer, InternalRow> columnGroupRows,
                List<DataField> readDataFields) {
            this.columnGroupNum = columnGroupNum;
            this.columnGroupRows = columnGroupRows;
            this.readDataFields = readDataFields;
            this.columnGroupRowMapping = getColumnGroupRowMapping();
        }

        @Override
        public int getFieldCount() {
            return readDataFields.size();
        }

        @Override
        public RowKind getRowKind() {
            return columnGroupRows.get(0).getRowKind();
        }

        @Override
        public void setRowKind(RowKind kind) {
            columnGroupRows.get(0).setRowKind(kind);
        }

        @Override
        public boolean isNullAt(int pos) {
            return opColumnGroupRow(pos, DataGetters::isNullAt);
        }

        @Override
        public boolean getBoolean(int pos) {
            return opColumnGroupRow(pos, DataGetters::getBoolean);
        }

        @Override
        public byte getByte(int pos) {
            return opColumnGroupRow(pos, DataGetters::getByte);
        }

        @Override
        public short getShort(int pos) {
            return opColumnGroupRow(pos, DataGetters::getShort);
        }

        @Override
        public int getInt(int pos) {
            return opColumnGroupRow(pos, DataGetters::getInt);
        }

        @Override
        public long getLong(int pos) {
            return opColumnGroupRow(pos, DataGetters::getLong);
        }

        @Override
        public float getFloat(int pos) {
            return opColumnGroupRow(pos, DataGetters::getFloat);
        }

        @Override
        public double getDouble(int pos) {
            return opColumnGroupRow(pos, DataGetters::getDouble);
        }

        @Override
        public BinaryString getString(int pos) {
            return opColumnGroupRow(pos, DataGetters::getString);
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale) {
            return opColumnGroupRow(
                    pos, (row, posInGroup) -> row.getDecimal(posInGroup, precision, scale));
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision) {
            return opColumnGroupRow(
                    pos, (row, posInGroup) -> row.getTimestamp(posInGroup, precision));
        }

        @Override
        public byte[] getBinary(int pos) {
            return opColumnGroupRow(pos, DataGetters::getBinary);
        }

        @Override
        public Variant getVariant(int pos) {
            return opColumnGroupRow(pos, DataGetters::getVariant);
        }

        @Override
        public InternalArray getArray(int pos) {
            return opColumnGroupRow(pos, DataGetters::getArray);
        }

        @Override
        public InternalMap getMap(int pos) {
            return opColumnGroupRow(pos, DataGetters::getMap);
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            return opColumnGroupRow(pos, (row, posInGroup) -> row.getRow(pos, numFields));
        }

        private int[][] getColumnGroupRowMapping() {
            int[][] mapping = new int[readDataFields.size()][2];

            int[] columnGroupCount = new int[columnGroupNum];
            Arrays.fill(columnGroupCount, 0);

            for (int i = 0; i < readDataFields.size(); i++) {
                DataField field = readDataFields.get(i);
                int columnGroupId = field.getColumnGroupId();
                mapping[i][0] = columnGroupId;
                mapping[i][1] = columnGroupCount[columnGroupId]++;
            }

            return mapping;
        }

        private <T> T opColumnGroupRow(int pos, BiFunction<InternalRow, Integer, T> op) {
            return op.apply(
                    columnGroupRows.get(columnGroupRowMapping[pos][0]),
                    columnGroupRowMapping[pos][1]);
        }
    }
}
