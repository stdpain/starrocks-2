// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/gensrc/thrift/Types.thrift

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

namespace cpp starrocks
namespace java com.starrocks.thrift

include "CloudConfiguration.thrift"
include "Types.thrift"

enum TFunctionType {
  SCALAR,
  AGGREGATE,
}

enum TFunctionBinaryType {
  // StarRocks builtin. We can either run this interpreted or via codegen
  // depending on the query option.
  BUILTIN,

  // Hive UDFs, loaded from *.jar
  HIVE,

  // Native-interface, precompiled UDFs loaded from *.so
  NATIVE,

  // Native-interface, precompiled to IR; loaded from *.ll
  IR,

  // StarRocks customized UDF in jar.
  SRJAR,

  //
  PYTHON
}

// Represents a fully qualified function name.
struct TFunctionName {
  // Name of the function's parent database. Not set if in global
  // namespace (e.g. builtins)
  1: optional string db_name

  // Name of the function
  2: required string function_name
}

struct TScalarFunction {
    // Symbol for the function
    1: required string symbol
    2: optional string prepare_fn_symbol
    3: optional string close_fn_symbol
}

struct TAggregateFunction {
  1: required Types.TTypeDesc intermediate_type
  2: optional string update_fn_symbol
  3: optional string init_fn_symbol
  4: optional string serialize_fn_symbol
  5: optional string merge_fn_symbol
  6: optional string finalize_fn_symbol
  8: optional string get_value_fn_symbol
  9: optional string remove_fn_symbol
  10: optional bool is_analytic_only_fn = false
  11: optional string symbol
  // used for agg_func(a order by b, c) like array_agg, group_concat
  12: optional list<bool> is_asc_order
  // Indicates, for each expr, if nulls should be listed first or last. This is
  // independent of is_asc_order.
  13: optional list<bool> nulls_first
  14: optional bool is_distinct = false
}

struct TTableFunction {
  1: required list<Types.TTypeDesc> ret_types
  2: optional string symbol
  // Table function left join
  3: optional bool is_left_join
}

struct TAggStateDesc {
    1: optional string agg_func_name
    2: optional list<Types.TTypeDesc> arg_types
    3: optional Types.TTypeDesc ret_type
    4: optional bool result_nullable
    5: optional i32 func_version
}

// Represents a function in the Catalog.
struct TFunction {
  // Fully qualified function name.
  1: required TFunctionName name

  // Type of the udf. e.g. hive, native, ir
  2: required TFunctionBinaryType binary_type

  // The types of the arguments to the function
  3: required list<Types.TTypeDesc> arg_types

  // Return type for the function.
  4: required Types.TTypeDesc ret_type

  // If true, this function takes var args.
  5: required bool has_var_args

  // Optional comment to attach to the function
  6: optional string comment

  7: optional string signature // Deprecated

  // HDFS path for the function binary. This binary must exist at the time the
  // function is created.
  8: optional string hdfs_location

  // One of these should be set.
  9: optional TScalarFunction scalar_fn
  10: optional TAggregateFunction aggregate_fn

  11: optional i64 id
  12: optional string checksum
  13: optional TAggStateDesc agg_state_desc

  // Builtin Function id, used to mark the function in the vectorization engine,
  // and it's different with `id` because `id` is use for serialized and cache
  // UDF function.
  30: optional i64 fid
  31: optional TTableFunction table_fn
  32: optional bool could_apply_dict_optimize

  // Ignore nulls
  33: optional bool ignore_nulls
  34: optional bool isolated
  35: optional string input_type
  36: optional string content
  37: optional CloudConfiguration.TCloudConfiguration cloud_configuration
}
