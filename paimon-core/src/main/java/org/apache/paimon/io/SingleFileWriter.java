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

import org.apache.paimon.fs.Path;

import java.io.IOException;

/**
 * File writer to accept one record or a bunch of records, write to a single logical file and
 * generate metadata after closing it.
 *
 * @param <T> record type.
 * @param <R> file result to collect.
 */
// TODO: Proper naming of the class
public interface SingleFileWriter<T, R> extends FileWriter<T, R> {

    void writeBundle(BundleRecords bundle) throws IOException;

    boolean reachTargetSize(boolean b, long targetFileSize) throws IOException;

    Path path();

    AbortExecutor abortExecutor();

    /** AbortExecutor. */
    interface AbortExecutor {
        void abort();
    }
}
