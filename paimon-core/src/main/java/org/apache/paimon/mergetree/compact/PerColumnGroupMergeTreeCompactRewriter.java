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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.PerColumnGroupRollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.PerColumnGroupMergeSorter;
import org.apache.paimon.mergetree.PerColumnGroupMergeTreeReaders;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.reader.PerColumnGroupRecordReader;
import org.apache.paimon.reader.PerColumnGroupRecordReaderIterator;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/** Default {@link CompactRewriter} for merge trees. */
public class PerColumnGroupMergeTreeCompactRewriter extends AbstractCompactRewriter {

    protected final FileReaderFactory<KeyValue> readerFactory;
    protected final KeyValueFileWriterFactory writerFactory;
    protected final Comparator<InternalRow> keyComparator;
    @Nullable protected final FieldsComparator userDefinedSeqComparator;
    protected final MergeFunctionFactory<KeyValue> mfFactory;
    protected final MergeSorter mergeSorter;
    private final int columnGroupNum;

    public PerColumnGroupMergeTreeCompactRewriter(
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            int columnGroupNum) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.keyComparator = keyComparator;
        this.userDefinedSeqComparator = userDefinedSeqComparator;
        this.mfFactory = mfFactory;
        this.mergeSorter = mergeSorter;
        this.columnGroupNum = columnGroupNum;
    }

    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        return rewriteCompaction(outputLevel, dropDelete, sections);
    }

    protected CompactResult rewriteCompaction(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        PerColumnGroupRollingFileWriter writer =
                (PerColumnGroupRollingFileWriter)
                        writerFactory.createPerColumnGroupRollingMergeTreeFileWriter(
                                outputLevel, FileSource.COMPACT);
        PerColumnGroupRecordReader<KeyValue> reader = null;
        Exception collectedExceptions = null;
        try {
            reader =
                    readerForMergeTree(
                            columnGroupNum,
                            sections,
                            (columnGroupId) ->
                                    new ReducerMergeFunctionWrapper(
                                            mfFactory.createForColumnGroup(columnGroupId)));

            if (dropDelete) {
                reader = new DropDeleteReader(reader);
            }

            for (int i = 0; i < columnGroupNum; i++) {
                writer.setCurrentColumnGroupId(i);
                PerColumnGroupRecordReaderIterator<KeyValue> iter =
                        new PerColumnGroupRecordReaderIterator<>(reader);
                writer.write(iter);

                if (i < columnGroupNum - 1) {
                    iter.advanceToNextColumnGroup();
                }
            }
        } catch (Exception e) {
            collectedExceptions = e;
        } finally {
            try {
                IOUtils.closeAll(reader, writer);
            } catch (Exception e) {
                collectedExceptions = ExceptionUtils.firstOrSuppressed(e, collectedExceptions);
            }
        }

        if (null != collectedExceptions) {
            writer.abort();
            throw collectedExceptions;
        }

        List<DataFileMeta> before = extractFilesFromSections(sections);
        notifyRewriteCompactBefore(before);
        return new CompactResult(before, writer.result());
    }

    protected <T> PerColumnGroupRecordReader<T> readerForMergeTree(
            int numColumnGroup,
            List<List<SortedRun>> sections,
            MergeFunctionWrapperFactory<T> mergeFunctionWrapper)
            throws IOException {
        return PerColumnGroupMergeTreeReaders.readerForMergeTree(
                numColumnGroup,
                sections,
                readerFactory,
                keyComparator,
                userDefinedSeqComparator,
                mergeFunctionWrapper,
                new PerColumnGroupMergeSorter(mergeSorter));
    }

    protected void notifyRewriteCompactBefore(List<DataFileMeta> files) {}

    /** Wrapper for {@link MergeFunction}. */
    public interface MergeFunctionWrapperFactory<T> {
        MergeFunctionWrapper<T> get(int columnGroupId);
    }
}
