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

import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** A {@link FileWriter} that writes data to multiple column group files. */
public class PerColumnGroupRollingFileWriter implements FileWriter<KeyValue, List<DataFileMeta>> {
    private static final Logger LOG = LoggerFactory.getLogger(RollingFileWriter.class);

    private static final int CHECK_ROLLING_RECORD_CNT = 1000;

    private final PerColumnGroupRollingFileWriter.ColumnGroupFileWriterFactory writerFactory;
    private final long targetFileSize;
    private final List<StatsCollectingPerColumnGroupFileWriter.AbortExecutor> closedWriters;
    private final List<DataFileMeta> results;

    private StatsCollectingPerColumnGroupFileWriter currentWriter;

    private long recordCount = 0;
    private boolean closed = false;
    private int currentColumnGroupId;

    public PerColumnGroupRollingFileWriter(
            PerColumnGroupRollingFileWriter.ColumnGroupFileWriterFactory writerFactory,
            long targetFileSize) {
        this.writerFactory = writerFactory;
        this.targetFileSize = targetFileSize;
        this.results = new ArrayList<>();
        this.closedWriters = new ArrayList<>();
    }

    @VisibleForTesting
    public long targetFileSize() {
        return targetFileSize;
    }

    @VisibleForTesting
    boolean rollingFile() throws IOException {
        // TODO: Support rolling
        return false;
        //        return currentWriter.reachTargetSize(
        //                recordCount % CHECK_ROLLING_RECORD_CNT == 0, targetFileSize);
    }

    @Override
    public void write(KeyValue row) throws IOException {
        if (closed) {
            throw new RuntimeException("Writer has already closed!");
        }

        if (currentWriter == null) {
            openWriter();
        }
        try {
            currentWriter.write(row);
            recordCount += 1;

            if (rollingFile()) {
                closeWriter();
            }
        } catch (Throwable e) {
            LOG.warn("Exception occurs when writing file. Cleaning up.", e);
            abort();
            throw e;
        }
    }

    public void setCurrentColumnGroupId(int currentColumnGroupId) throws IOException {
        closeWriter();
        this.currentColumnGroupId = currentColumnGroupId;
        openWriter();
    }

    private void openWriter() {
        currentWriter = writerFactory.get(currentColumnGroupId);
    }

    private void closeWriter() throws IOException {
        if (currentWriter == null) {
            return;
        }

        currentWriter.close();
        // only store abort executor in memory
        // cannot store whole writer, it includes lots of memory for example column vectors to
        // read
        // and write
        closedWriters.add(currentWriter.abortExecutor());
        if (currentColumnGroupId == 0) {
            results.add(currentWriter.result());
        }
        this.currentWriter = null;
    }

    @Override
    public long recordCount() {
        return recordCount;
    }

    @Override
    public void abort() {
        if (currentWriter != null) {
            currentWriter.abort();
        }
        for (StatsCollectingPerColumnGroupFileWriter.AbortExecutor abortExecutor : closedWriters) {
            abortExecutor.abort();
        }
    }

    @Override
    public List<DataFileMeta> result() {
        Preconditions.checkState(closed, "Cannot access the results unless close all writers.");
        return results;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            closeWriter();
        } catch (IOException e) {
            LOG.warn("Exception occurs when writing file. Cleaning up.", e);
            abort();
            throw e;
        } finally {
            closed = true;
        }
    }

    /** Factory to create {@link StatsCollectingPerColumnGroupFileWriter}. */
    public interface ColumnGroupFileWriterFactory {
        StatsCollectingPerColumnGroupFileWriter get(int columnGroupId);
    }
}
