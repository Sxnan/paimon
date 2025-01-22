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

package org.apache.paimon.reader;

import org.apache.paimon.utils.CloseableIterator;

import java.io.IOException;

/** ColumnGroupedRecordReaderIterator. */
public class PerColumnGroupRecordReaderIterator<T> implements CloseableIterator<T> {
    private final PerColumnGroupRecordReader<T> reader;
    private RecordReader.RecordIterator<T> currentIterator;
    private boolean advanced;
    private T currentResult;

    public PerColumnGroupRecordReaderIterator(PerColumnGroupRecordReader<T> reader)
            throws IOException {
        this.reader = reader;
        this.advanced = false;
        this.currentResult = null;

        this.currentIterator = reader.readBatch();
    }

    public void advanceToNextColumnGroup() throws Exception {
        reader.nextColumnGroup();
        this.currentIterator = reader.readBatch();
    }

    /**
     * <b>IMPORTANT</b>: Before calling this, make sure that the previous returned key-value is not
     * used any more!
     */
    @Override
    public boolean hasNext() {
        if (currentIterator == null) {
            return false;
        }
        advanceIfNeeded();
        return currentResult != null;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            return null;
        }
        advanced = false;
        //        System.out.println(currentResult);
        return currentResult;
    }

    private void advanceIfNeeded() {
        if (advanced) {
            return;
        }
        advanced = true;

        try {
            while (true) {
                currentResult = currentIterator.next();
                if (currentResult != null) {
                    break;
                } else {
                    currentIterator.releaseBatch();
                    // because reader#readBatch will be affected by interrupt, which will cause
                    // currentIterator#releaseBatch to be executed twice.
                    currentIterator = null;
                    currentIterator = reader.readBatch();
                    if (currentIterator == null) {
                        break;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (currentIterator != null) {
                currentIterator.releaseBatch();
            }
        } finally {
            reader.close();
        }
    }
}
