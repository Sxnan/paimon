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

import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.casting.CastedRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.PerColumnGroupRecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.FormatReaderMapping;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** Reads {@link InternalRow} from data files. */
public class PerColumnGroupDataFileRecordReader
        implements FileRecordReader<InternalRow>, PerColumnGroupRecordReader<InternalRow> {

    private final BulkFormatMappingFactory bulkFormatMappingFactory;
    private final FormatReaderContextFactory contextFactory;
    private final Path path;
    private final TableSchema schema;
    private final List<DataField> dataFields;
    @Nullable private final int[] indexMapping;
    @Nullable private final PartitionInfo partitionInfo;
    @Nullable private final CastFieldGetter[] castMapping;

    private FileRecordReader<InternalRow> currentReader;
    private int currentColumnGroupId;

    public PerColumnGroupDataFileRecordReader(
            BulkFormatMappingFactory bulkFormatMappingFactory,
            FormatReaderContextFactory contextFactory,
            Path path,
            TableSchema schema,
            List<DataField> dataFields,
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable PartitionInfo partitionInfo)
            throws IOException {
        this.bulkFormatMappingFactory = bulkFormatMappingFactory;
        this.contextFactory = contextFactory;
        this.path = path;
        this.currentReader = null;
        this.currentColumnGroupId = 0;
        this.schema = schema;
        this.dataFields = dataFields;
        this.indexMapping = indexMapping;
        this.partitionInfo = partitionInfo;
        this.castMapping = castMapping;
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {

        if (currentReader == null) {
            FormatReaderFactory.Context context =
                    contextFactory.get(toColumnGroupPath(currentColumnGroupId, path));
            currentReader =
                    bulkFormatMappingFactory
                            .get(currentColumnGroupId)
                            .getReaderFactory()
                            .createReader(context);
        }

        FileRecordIterator<InternalRow> iterator = currentReader.readBatch();
        if (iterator == null) {
            currentReader.close();
            currentReader = null;
            return null;
        }

        //        if (iterator instanceof ColumnarRowIterator) {
        //            iterator = ((ColumnarRowIterator) iterator).mapping(partitionInfo,
        // indexMapping);
        //        } else {
        //            if (partitionInfo != null) {
        //                final PartitionSettedRow partitionSettedRow =
        //                        PartitionSettedRow.from(partitionInfo);
        //                iterator = iterator.transform(partitionSettedRow::replaceRow);
        //            }
        //            if (indexMapping != null) {
        //                final ProjectedRow projectedRow = ProjectedRow.from(indexMapping);
        //                iterator = iterator.transform(projectedRow::replaceRow);
        //            }
        //        }

        if (castMapping != null) {
            final CastedRow castedRow = CastedRow.from(castMapping);
            iterator = iterator.transform(castedRow::replaceRow);
        }

        return iterator;
    }

    @Override
    public void close() throws IOException {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
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

    @Override
    public int nextColumnGroup() throws IOException {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
        }

        currentColumnGroupId++;
        return currentColumnGroupId;
    }

    /** Factory to create {@link FormatReaderFactory.Context}. */
    public interface FormatReaderContextFactory {
        FormatReaderFactory.Context get(Path path) throws IOException;
    }

    /** Factory to create {@link FormatReaderMapping}. */
    public interface BulkFormatMappingFactory {
        FormatReaderMapping get(int columnGroupId);
    }
}
