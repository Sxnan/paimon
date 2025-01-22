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

package org.apache.paimon;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.OffsetRow;
import org.apache.paimon.utils.ProjectedRow;

import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for {@link KeyValue}.
 *
 * <p>NOTE: {@link InternalRow} and {@link KeyValue} produced by this serializer are reused.
 */
public class PerColumnGroupKeyValueSerializer extends ObjectSerializer<KeyValue> {

    private static final long serialVersionUID = 1L;

    private final int keyArity;

    private final GenericRow reusedMeta;
    private final JoinedRow reusedKeyWithMeta;
    private final JoinedRow reusedRow;

    private final OffsetRow reusedKey;
    private final ProjectedRow reusedValue;
    private final KeyValue reusedKv;
    private final int columnGroupId;

    public PerColumnGroupKeyValueSerializer(int columnGroupId, RowType keyType, RowType valueType) {
        super(KeyValue.schema(columnGroupId, keyType, valueType));
        this.columnGroupId = columnGroupId;

        List<Integer> valueIndexMapping = new ArrayList<>();
        if (columnGroupId != 0) {
            keyType = RowType.of();
            int currentIdx = 0;
            for (int i = 0; i < valueType.getFieldCount(); i++) {
                DataField field = valueType.getField(i);
                if (field.getColumnGroupId() == columnGroupId) {
                    valueIndexMapping.add(currentIdx++);
                }
            }
        }

        this.keyArity = keyType.getFieldCount();

        if (columnGroupId == 0) {
            this.reusedMeta = new GenericRow(2);
        } else {
            this.reusedMeta = new GenericRow(0);
        }
        this.reusedKeyWithMeta = new JoinedRow();
        this.reusedRow = new JoinedRow();

        this.reusedKey = new OffsetRow(keyArity, 0);
        this.reusedValue = ProjectedRow.from(valueIndexMapping.stream().mapToInt(i -> i).toArray());
        this.reusedKv = new KeyValue().replace(reusedKey, -1, null, reusedValue);
    }

    @Override
    public InternalRow toRow(KeyValue record) {
        return toRow(record.key(), record.sequenceNumber(), record.valueKind(), record.value());
    }

    public InternalRow toRow(
            InternalRow key, long sequenceNumber, RowKind valueKind, InternalRow value) {
        if (columnGroupId == 0) {
            reusedMeta.setField(0, sequenceNumber);
            reusedMeta.setField(1, valueKind.toByteValue());
        }
        return reusedRow.replace(reusedKeyWithMeta.replace(key, reusedMeta), value);
    }

    @Override
    public KeyValue fromRow(InternalRow row) {

        if (columnGroupId == 0) {
            reusedKey.replace(row);
            long sequenceNumber = row.getLong(keyArity);
            RowKind valueKind = RowKind.fromByteValue(row.getByte(keyArity + 1));
            return reusedKv.replace(reusedKey, sequenceNumber, valueKind, GenericRow.of());
        } else {
            reusedValue.replaceRow(row);
            return reusedKv.replace(GenericRow.of(), 0, RowKind.INSERT, reusedValue);
        }
    }

    public KeyValue getReusedKv() {
        return reusedKv;
    }
}
