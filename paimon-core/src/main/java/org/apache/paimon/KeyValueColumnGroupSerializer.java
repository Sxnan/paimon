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

/**
 * Serializer for {@link KeyValue}. It is used to serialize {@link KeyValue} to {@link InternalRow}
 * for the given columnGroupId.
 *
 * <p>NOTE: {@link InternalRow} and {@link KeyValue} produced by this serializer are reused.
 */
public class KeyValueColumnGroupSerializer extends ObjectSerializer<KeyValue> {

    private static final long serialVersionUID = 1L;
    private static final InternalRow EMTPY_ROW = new GenericRow(0);

    private final int columnGroupId;
    private final int keyArity;

    private final GenericRow reusedMeta;
    private final JoinedRow reusedKeyWithMeta;
    private final ProjectedRow reusedColumnGroupValue;
    private final JoinedRow reusedRow;

    private final OffsetRow reusedKey;
    private final OffsetRow reusedValue;
    private final KeyValue reusedKv;

    public KeyValueColumnGroupSerializer(int columnGroupId, RowType keyType, RowType valueType) {
        super(KeyValue.schema(keyType, valueType));
        this.columnGroupId = columnGroupId;

        this.keyArity = keyType.getFieldCount();
        int valueArity = valueType.getFieldCount();

        this.reusedMeta = new GenericRow(2);
        this.reusedKeyWithMeta = new JoinedRow();
        this.reusedColumnGroupValue =
                ProjectedRow.from(getValueIndexMappingForColumnGroup(columnGroupId, valueType));
        this.reusedRow = new JoinedRow();

        this.reusedKey = new OffsetRow(keyArity, 0);
        this.reusedValue = new OffsetRow(valueArity, keyArity + 2);
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
            return reusedRow.replace(reusedKeyWithMeta.replace(key, reusedMeta), EMTPY_ROW);
        } else {
            return reusedRow.replace(EMTPY_ROW, reusedColumnGroupValue.replaceRow(value));
        }
    }

    @Override
    public KeyValue fromRow(InternalRow row) {
        reusedKey.replace(row);
        reusedValue.replace(row);
        long sequenceNumber = row.getLong(keyArity);
        RowKind valueKind = RowKind.fromByteValue(row.getByte(keyArity + 1));
        reusedKv.replace(reusedKey, sequenceNumber, valueKind, reusedValue);
        return reusedKv;
    }

    public static int[] getValueIndexMappingForColumnGroup(int columnGroupId, RowType valueType) {
        ArrayList<Integer> columnGroupFieldIdx = new ArrayList<>();
        if (columnGroupId == 0) {
            return new int[0];
        }

        for (int i = 0; i < valueType.getFieldCount(); i++) {
            DataField field = valueType.getField(i);
            if (field.getColumnGroupId() == columnGroupId) {
                columnGroupFieldIdx.add(i);
            }
        }
        return columnGroupFieldIdx.stream().mapToInt(i -> i).toArray();
    }
}
