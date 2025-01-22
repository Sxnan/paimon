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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.PerColumnGroupMergeTreeCompactRewriter;
import org.apache.paimon.reader.PerColumnGroupReaderSupplier;
import org.apache.paimon.reader.PerColumnGroupRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.SizedReaderSupplier;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/** MergeSorter for per column group. */
public class PerColumnGroupMergeSorter {
    private final MergeSorter mergeSorter;

    public PerColumnGroupMergeSorter(MergeSorter mergeSorter) {
        this.mergeSorter = mergeSorter;
    }

    public <T> PerColumnGroupRecordReader<T> mergeSort(
            List<PerColumnGroupReaderSupplier<KeyValue>> readers,
            Comparator<InternalRow> userKeyComparator,
            FieldsComparator userDefinedSeqComparator,
            PerColumnGroupMergeTreeCompactRewriter.MergeFunctionWrapperFactory<T>
                    mergeFunctionWrapper)
            throws IOException {
        return new PerColumnGroupMergeSortReader<>(
                readers,
                userKeyComparator,
                userDefinedSeqComparator,
                mergeFunctionWrapper,
                mergeSorter);
    }

    private static class PerColumnGroupMergeSortReader<T> implements PerColumnGroupRecordReader<T> {

        private final List<PerColumnGroupRecordReader<KeyValue>> readers;
        private final Comparator<InternalRow> userKeyComparator;
        private final FieldsComparator userDefinedSeqComparator;
        private final PerColumnGroupMergeTreeCompactRewriter.MergeFunctionWrapperFactory<T>
                mergeFunctionWrapperFactory;
        private final MergeSorter mergeSorter;

        private final List<SortMergeAction> actionSequence;
        private RecordReader<T> currentReader;
        private MergeFunctionWrapper<T> currentMergeFunctionWrapper;

        private int currentColumnGroupId;
        private boolean currentColumnGroupEnded;

        public PerColumnGroupMergeSortReader(
                List<PerColumnGroupReaderSupplier<KeyValue>> readers,
                Comparator<InternalRow> userKeyComparator,
                FieldsComparator userDefinedSeqComparator,
                PerColumnGroupMergeTreeCompactRewriter.MergeFunctionWrapperFactory<T>
                        mergeFunctionWrapperFactory,
                MergeSorter mergeSorter)
                throws IOException {
            this.readers = new ArrayList<>();
            for (PerColumnGroupReaderSupplier<KeyValue> readerSupplier : readers) {
                this.readers.add(readerSupplier.get());
            }
            this.userKeyComparator = userKeyComparator;
            this.userDefinedSeqComparator = userDefinedSeqComparator;
            this.mergeFunctionWrapperFactory = mergeFunctionWrapperFactory;
            this.mergeSorter = mergeSorter;

            this.actionSequence = new ArrayList<>();
            this.currentColumnGroupId = 0;
            this.currentColumnGroupEnded = false;
        }

        @Nullable
        @Override
        public RecordIterator<T> readBatch() throws IOException {

            if (currentColumnGroupEnded) {
                return null;
            }

            if (currentReader == null && currentColumnGroupId == 0) {
                List<SizedReaderSupplier<KeyValue>> lazyReaders =
                        readers.stream()
                                .map(
                                        reader ->
                                                new SizedReaderSupplier<KeyValue>() {
                                                    @Override
                                                    public RecordReader<KeyValue> get() {
                                                        return reader;
                                                    }

                                                    @Override
                                                    public long estimateSize() {
                                                        // TODO: Implement estimateSize
                                                        return 0;
                                                    }
                                                })
                                .collect(Collectors.toList());
                currentMergeFunctionWrapper = mergeFunctionWrapperFactory.get(currentColumnGroupId);
                currentReader =
                        mergeSorter.mergeSort(
                                lazyReaders,
                                userKeyComparator,
                                userDefinedSeqComparator,
                                currentMergeFunctionWrapper,
                                new SortMergeActionListener<KeyValue>() {
                                    @Override
                                    public void onReadFromReader(RecordReader<KeyValue> reader) {
                                        actionSequence.add(new SortMergeReadAction<>(reader));
                                    }

                                    @Override
                                    public void onMerge(InternalRow mergeKey) {
                                        actionSequence.add(new SortMergeMergeAction(mergeKey));
                                    }
                                });
            } else if (currentReader == null) {
                currentMergeFunctionWrapper = mergeFunctionWrapperFactory.get(currentColumnGroupId);
                currentReader =
                        new ReplayReaderWrapper<>(
                                actionSequence,
                                currentMergeFunctionWrapper,
                                () -> {
                                    currentColumnGroupEnded = true;
                                    return null;
                                });
            }

            return currentReader.readBatch();
        }

        @Override
        public void close() throws IOException {}

        @Override
        public int nextColumnGroup() throws Exception {
            int nextColumnGroup = -1;
            for (PerColumnGroupRecordReader<KeyValue> reader : readers) {
                Preconditions.checkState(reader != null);
                int readerColumnGroup = reader.nextColumnGroup();
                if (nextColumnGroup == -1) {
                    nextColumnGroup = readerColumnGroup;
                } else {
                    Preconditions.checkState(nextColumnGroup == readerColumnGroup);
                }
            }
            currentColumnGroupId = nextColumnGroup;
            currentColumnGroupEnded = false;
            currentReader = null;
            return nextColumnGroup;
        }
    }

    private static class ReplayReaderWrapper<T> implements RecordReader<T> {
        private final List<SortMergeAction> sortMergeActions;
        private final MergeFunctionWrapper<T> mergeFunctionWrapper;
        private final Callable<Void> columnGroupEndedAction;

        private ReplayReaderWrapper(
                List<SortMergeAction> sortMergeActions,
                MergeFunctionWrapper<T> mergeFunctionWrapper,
                Callable<Void> columnGroupEndedAction) {
            this.sortMergeActions = sortMergeActions;
            this.mergeFunctionWrapper = mergeFunctionWrapper;
            this.columnGroupEndedAction = columnGroupEndedAction;
        }

        @Nullable
        @Override
        public RecordIterator<T> readBatch() throws IOException {
            return new ReplayReaderIterator<>(
                    sortMergeActions, mergeFunctionWrapper, columnGroupEndedAction);
        }

        @Override
        public void close() throws IOException {}

        private static class ReplayReaderIterator<T> implements RecordIterator<T> {
            private final MergeFunctionWrapper<T> mergeFunctionWrapper;
            private final Callable<Void> columnGroupEndedAction;
            private final Map<RecordReader<KeyValue>, RecordIterator<KeyValue>> iterators;
            private final Iterator<SortMergeAction> actionIterator;

            public ReplayReaderIterator(
                    List<SortMergeAction> sortMergeActions,
                    MergeFunctionWrapper<T> mergeFunctionWrapper,
                    Callable<Void> columnGroupEndedAction) {
                this.mergeFunctionWrapper = mergeFunctionWrapper;
                this.columnGroupEndedAction = columnGroupEndedAction;
                this.iterators = new HashMap<>();
                this.actionIterator = sortMergeActions.iterator();
            }

            @Nullable
            @Override
            public T next() throws IOException {
                mergeFunctionWrapper.reset();
                while (true) {
                    if (!actionIterator.hasNext()) {
                        return null;
                    }

                    SortMergeAction action = actionIterator.next();
                    if (action instanceof SortMergeMergeAction) {
                        T result = mergeFunctionWrapper.getResult();
                        if (result == null) {
                            continue;
                        }
                        return result;
                    } else {
                        RecordReader<KeyValue> reader =
                                ((SortMergeReadAction<KeyValue>) action).getReader();

                        while (true) {
                            if (iterators.get(reader) == null) {
                                RecordIterator<KeyValue> iter = reader.readBatch();
                                if (iter == null) {
                                    try {
                                        columnGroupEndedAction.call();
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                    return null;
                                }
                                iterators.put(reader, iter);
                            } else {
                                KeyValue next = iterators.get(reader).next();
                                if (next == null) {
                                    iterators.get(reader).releaseBatch();
                                    iterators.put(reader, null);
                                    continue;
                                } else {
                                    mergeFunctionWrapper.add(next);
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            @Override
            public void releaseBatch() {
                for (RecordIterator<KeyValue> iter : iterators.values()) {
                    if (iter != null) {
                        iter.releaseBatch();
                    }
                }

                iterators.clear();
            }
        }
    }

    /** A {@link SortMergeAction} that can be used to trigger a merge. */
    public interface SortMergeAction {}

    /** A {@link SortMergeAction} that can be used to read from a {@link RecordReader}. */
    public static class SortMergeReadAction<T> implements SortMergeAction {
        private final RecordReader<T> reader;

        public SortMergeReadAction(RecordReader<T> reader) {
            this.reader = reader;
        }

        public RecordReader<T> getReader() {
            return reader;
        }

        @Override
        public String toString() {
            return String.format("Read from reader: %s", reader);
        }
    }

    /** A {@link SortMergeAction} that can be used to trigger a merge. */
    public static class SortMergeMergeAction implements SortMergeAction {
        private final InternalRow mergeKey;

        public SortMergeMergeAction(InternalRow mergeKey) {
            this.mergeKey = mergeKey;
        }

        @Override
        public String toString() {
            return String.format("Merge for key %s", mergeKey);
        }
    }

    /** A listener that can be used to listen to {@link SortMergeAction}s. */
    public interface SortMergeActionListener<T> {

        void onReadFromReader(RecordReader<T> reader);

        void onMerge(InternalRow mergeKey);
    }
}
