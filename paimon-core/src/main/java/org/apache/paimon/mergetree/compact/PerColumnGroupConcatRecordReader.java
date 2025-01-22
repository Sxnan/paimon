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

import org.apache.paimon.reader.PerColumnGroupReaderSupplier;
import org.apache.paimon.reader.PerColumnGroupRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * This reader is to concatenate a list of {@link RecordReader}s and read them sequentially. The
 * input list is already sorted by key and sequence number, and the key intervals do not overlap
 * each other.
 */
public class PerColumnGroupConcatRecordReader<T> implements PerColumnGroupRecordReader<T> {

    private final List<? extends PerColumnGroupReaderSupplier<T>> readerFactories;
    private final PerColumnGroupRecordReader<T>[] readers;
    private int currentReaderIdx;

    protected PerColumnGroupConcatRecordReader(
            int columnGroupNum, List<? extends PerColumnGroupReaderSupplier<T>> readerFactories) {
        readerFactories.forEach(
                supplier ->
                        Preconditions.checkNotNull(supplier, "Reader factory must not be null."));

        this.readerFactories = readerFactories;
        this.currentReaderIdx = 0;

        this.readers = new PerColumnGroupRecordReader[readerFactories.size()];
    }

    public static <R> PerColumnGroupRecordReader<R> create(
            int numColumnGroup, List<? extends PerColumnGroupReaderSupplier<R>> readers)
            throws IOException {
        return readers.size() == 1
                ? readers.get(0).get()
                : new PerColumnGroupConcatRecordReader<>(numColumnGroup, readers);
    }

    public static <R> PerColumnGroupRecordReader<R> create(
            int numColumnGroup,
            PerColumnGroupReaderSupplier<R> reader1,
            PerColumnGroupReaderSupplier<R> reader2)
            throws IOException {
        return create(numColumnGroup, Arrays.asList(reader1, reader2));
    }

    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        while (true) {
            if (currentReaderIdx < readers.length && readers[currentReaderIdx] != null) {
                RecordIterator<T> iterator = readers[currentReaderIdx].readBatch();
                if (iterator != null) {
                    return iterator;
                }
                currentReaderIdx++;
            } else if (currentReaderIdx < readers.length) {
                readers[currentReaderIdx] = readerFactories.get(currentReaderIdx).get();
            } else {
                return null;
            }
        }
    }

    @Override
    public void close() throws IOException {
        for (PerColumnGroupRecordReader<T> reader : readers) {
            if (reader != null) {
                reader.close();
            }
        }
    }

    @Override
    public int nextColumnGroup() throws Exception {
        int nextColumnGroup = -1;
        for (PerColumnGroupRecordReader<T> reader : readers) {
            Preconditions.checkState(reader != null);
            int readerColumnGroup = reader.nextColumnGroup();
            if (nextColumnGroup == -1) {
                nextColumnGroup = readerColumnGroup;
            } else {
                Preconditions.checkState(nextColumnGroup == readerColumnGroup);
            }
        }
        return nextColumnGroup;
    }
}
