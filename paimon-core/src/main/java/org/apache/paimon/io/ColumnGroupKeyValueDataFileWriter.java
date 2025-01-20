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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import java.io.IOException;
import java.util.function.Function;

/** Writer for column group key-value data file. */
public class ColumnGroupKeyValueDataFileWriter extends AbstractSingleFileWriter<KeyValue, Void> {

    public ColumnGroupKeyValueDataFileWriter(
            FileIO fileIO,
            FormatWriterFactory factory,
            Path path,
            Function<KeyValue, InternalRow> converter,
            String compression,
            boolean asyncWrite) {
        super(fileIO, factory, path, converter, compression, asyncWrite);
    }

    @Override
    public Void result() throws IOException {
        return null;
    }
}
