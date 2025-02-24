/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.protocols.postgres.types;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import io.crate.common.collections.Lists2;
import io.crate.common.collections.MapBuilder;
import io.crate.types.ArrayType;
import io.crate.types.BitStringType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.RowType;
import io.crate.types.StringType;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class PGTypes {

    private static final Map<DataType<?>, PGType> CRATE_TO_PG_TYPES = MapBuilder.<DataType<?>, PGType>newLinkedHashMapBuilder()
        .put(DataTypes.BYTE, CharType.INSTANCE)
        .put(DataTypes.STRING, VarCharType.INSTANCE)
        .put(DataTypes.BOOLEAN, BooleanType.INSTANCE)
        .put(DataTypes.UNTYPED_OBJECT, JsonType.INSTANCE)
        .put(RowType.EMPTY, RecordType.EMPTY_RECORD)
        .put(DataTypes.SHORT, SmallIntType.INSTANCE)
        .put(DataTypes.INTEGER, IntegerType.INSTANCE)
        .put(DataTypes.LONG, BigIntType.INSTANCE)
        .put(DataTypes.FLOAT, RealType.INSTANCE)
        .put(DataTypes.DOUBLE, DoubleType.INSTANCE)
        .put(DataTypes.NUMERIC, NumericType.INSTANCE)
        .put(DataTypes.TIMETZ, TimeTZType.INSTANCE)
        .put(DataTypes.TIMESTAMPZ, TimestampZType.INSTANCE)
        .put(DataTypes.TIMESTAMP, TimestampType.INSTANCE)
        .put(DataTypes.DATE, DateType.INSTANCE)
        .put(DataTypes.IP, VarCharType.INSTANCE) // postgres has no IP type, so map it to varchar - it matches the client representation
        .put(DataTypes.UNDEFINED, VarCharType.INSTANCE)
        .put(DataTypes.GEO_SHAPE, JsonType.INSTANCE)
        .put(io.crate.types.JsonType.INSTANCE, JsonType.INSTANCE)
        .put(DataTypes.GEO_POINT, PointType.INSTANCE)
        .put(DataTypes.INTERVAL, IntervalType.INSTANCE)
        .put(DataTypes.REGPROC, RegprocType.INSTANCE)
        .put(DataTypes.REGCLASS, RegclassType.INSTANCE)
        .put(BitStringType.INSTANCE_ONE, BitType.INSTANCE)
        .put(new ArrayType<>(DataTypes.BYTE), PGArray.CHAR_ARRAY)
        .put(new ArrayType<>(DataTypes.SHORT), PGArray.INT2_ARRAY)
        .put(new ArrayType<>(DataTypes.INTEGER), PGArray.INT4_ARRAY)
        .put(new ArrayType<>(DataTypes.LONG), PGArray.INT8_ARRAY)
        .put(new ArrayType<>(DataTypes.FLOAT), PGArray.FLOAT4_ARRAY)
        .put(new ArrayType<>(DataTypes.DOUBLE), PGArray.FLOAT8_ARRAY)
        .put(new ArrayType<>(DataTypes.NUMERIC), PGArray.NUMERIC_ARRAY)
        .put(new ArrayType<>(DataTypes.BOOLEAN), PGArray.BOOL_ARRAY)
        .put(new ArrayType<>(DataTypes.TIMESTAMPZ), PGArray.TIMESTAMPZ_ARRAY)
        .put(new ArrayType<>(DataTypes.TIMESTAMP), PGArray.TIMESTAMP_ARRAY)
        .put(new ArrayType<>(DataTypes.DATE), PGArray.DATE_ARRAY)
        .put(new ArrayType<>(DataTypes.TIMETZ), PGArray.TIMETZ_ARRAY)
        .put(new ArrayType<>(DataTypes.STRING), PGArray.VARCHAR_ARRAY)
        .put(new ArrayType<>(DataTypes.IP), PGArray.VARCHAR_ARRAY)
        .put(new ArrayType<>(DataTypes.UNTYPED_OBJECT), PGArray.JSON_ARRAY)
        .put(new ArrayType<>(DataTypes.GEO_POINT), PGArray.POINT_ARRAY)
        .put(new ArrayType<>(DataTypes.GEO_SHAPE), PGArray.JSON_ARRAY)
        .put(new ArrayType<>(DataTypes.INTERVAL), PGArray.INTERVAL_ARRAY)
        .put(new ArrayType<>(RowType.EMPTY), PGArray.EMPTY_RECORD_ARRAY)
        .put(new ArrayType<>(DataTypes.REGPROC), PGArray.REGPROC_ARRAY)
        .put(new ArrayType<>(DataTypes.REGCLASS), PGArray.REGCLASS_ARRAY)
        .put(new ArrayType<>(BitStringType.INSTANCE_ONE), PGArray.BIT_ARRAY)
        .put(DataTypes.OIDVECTOR, PgOidVectorType.INSTANCE)
        .immutableMap();

    private static final IntObjectMap<DataType<?>> PG_TYPES_TO_CRATE_TYPE = new IntObjectHashMap<>();
    private static final Set<PGType> TYPES;

    static {
        for (Map.Entry<DataType<?>, PGType> e : CRATE_TO_PG_TYPES.entrySet()) {
            int oid = e.getValue().oid();
            // crate string and ip types both map to pg varchar, avoid overwriting the mapping that is first established.
            if (!PG_TYPES_TO_CRATE_TYPE.containsKey(oid)) {
                PG_TYPES_TO_CRATE_TYPE.put(oid, e.getKey());
            }
        }
        PG_TYPES_TO_CRATE_TYPE.put(0, DataTypes.UNDEFINED);
        PG_TYPES_TO_CRATE_TYPE.put(VarCharType.TextType.OID, DataTypes.STRING);
        PG_TYPES_TO_CRATE_TYPE.put(PGArray.TEXT_ARRAY.oid(), new ArrayType<>(DataTypes.STRING));
        PG_TYPES_TO_CRATE_TYPE.put(AnyType.OID, DataTypes.UNDEFINED);
        PG_TYPES_TO_CRATE_TYPE.put(PGArray.ANY_ARRAY.oid(), new ArrayType<>(DataTypes.UNDEFINED));
        PG_TYPES_TO_CRATE_TYPE.put(VarCharType.NameType.OID, DataTypes.STRING);
        PG_TYPES_TO_CRATE_TYPE.put(OidType.OID, DataTypes.INTEGER);
        TYPES = new HashSet<>(CRATE_TO_PG_TYPES.values()); // some pgTypes are used multiple times, de-dup them

        // There is no entry in `CRATE_TO_PG_TYPES` for these because we have no 1:1 mapping from dataType to pgType
        TYPES.add(AnyType.INSTANCE);
        TYPES.add(PGArray.ANY_ARRAY);
        TYPES.add(VarCharType.NameType.INSTANCE);
        TYPES.add(OidType.INSTANCE);

        // We map DataTypes.STRING to varchar (for no good reason, other than history)
        // But want to expose text additionally as well
        TYPES.add(VarCharType.TextType.INSTANCE);
        TYPES.add(PGArray.TEXT_ARRAY);
    }

    public static Iterable<PGType> pgTypes() {
        return TYPES;
    }

    @Nullable
    public static DataType<?> fromOID(int oid) {
        return PG_TYPES_TO_CRATE_TYPE.get(oid);
    }

    public static PGType get(DataType<?> type) {
        switch (type.id()) {
            case ArrayType.ID: {
                DataType<?> innerType = ((ArrayType<?>) type).innerType();
                if (innerType.id() == ArrayType.ID) {
                    // if this is a nested collection stream it as JSON because
                    // postgres binary format doesn't support multidimensional arrays
                    // with sub-arrays of different length
                    // (something like [ [1, 2], [3] ] is not supported)
                    return JsonType.INSTANCE;
                } else if (innerType.id() == ObjectType.ID) {
                    return PGArray.JSON_ARRAY;
                } else if (innerType.id() == RowType.ID) {
                    return new PGArray(PGArray.EMPTY_RECORD_ARRAY.oid(), get(innerType));
                }

                PGType pgType = CRATE_TO_PG_TYPES.get(type);
                if (pgType == null) {
                    PGType innerPGType = get(innerType);
                    if (innerPGType == null) {
                        throw new IllegalArgumentException(
                            String.format(Locale.ENGLISH, "No type mapping from '%s' to pg_type", type.getName()));
                    } else {
                        return new PGArray(innerPGType.typArray(), innerPGType);
                    }
                }
                return pgType;
            }
            case ObjectType.ID: {
                return JsonType.INSTANCE;
            }

            case RowType.ID:
                return new RecordType(Lists2.map(((RowType) type).fieldTypes(), PGTypes::get));

            case StringType.ID:
                return VarCharType.INSTANCE;

            case io.crate.types.NumericType.ID:
                return NumericType.INSTANCE;

            case BitStringType.ID:
                return new BitType(((BitStringType) type).length());

            default: {
                PGType pgType = CRATE_TO_PG_TYPES.get(type);
                if (pgType == null) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "No type mapping from '%s' to pg_type", type.getName()));
                }
                return pgType;
            }
        }

    }
}
