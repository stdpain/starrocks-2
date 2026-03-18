# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/common")

set(COMMON_FILES
  brpc_helper.cpp
  configbase.cpp
  config.cpp
  memory/column_allocator.cpp
  runtime_profile.cpp
  stack_util.cpp
  system/backend_options.cpp
  system/cpu_usage_info.cpp
  system/disk_info.cpp
  system/cpu_info.cpp
  system/cpu_info_debug.cpp
  system/mem_info.cpp
  thread/cpu_util.cpp
  thread/thread.cpp
  thread/threadpool.cpp
  s3_uri.cpp
  process_exit.cpp
  util/bthreads/executor.cpp
  util/debug_util.cpp
  util/misc.cpp
  util/stack_trace_mutex.cpp
  util/table_metrics.cpp
  util/thrift_client.cpp
  util/thrift_server.cpp
  util/thrift_util.cpp
  vlog_cntl.cpp
)

if (APPLE)
    list(APPEND COMMON_FILES tracer_mac_shim.cpp)
else()
    list(APPEND COMMON_FILES tracer.cpp)
endif()

if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
    list(APPEND COMMON_FILES util/minidump.cpp)
endif()

ADD_BE_LIB(Common
  ${COMMON_FILES}
)

# Generate env_config.h according to env_config.h.in
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/env_config.h.in ${GENSRC_DIR}/common/env_config.h)

# Auto-generate config_<module>_fwd.h from CONF_xxx module annotations in config.h.
# Output goes to ${GENSRC_DIR}/common/ so files are included as "common/config_*_fwd.h",
# consistent with how other generated headers (e.g. env_config.h) are consumed.
# Re-runs automatically whenever config.h or the generation script changes.
find_package(Python3 REQUIRED COMPONENTS Interpreter)
set(_GEN_CONFIG_FWD_SCRIPT "${CMAKE_SOURCE_DIR}/../build-support/gen_config_fwd_headers.py")
set(_CONFIG_FWD_STAMP "${CMAKE_CURRENT_BINARY_DIR}/config_fwd_headers.stamp")
add_custom_command(
    OUTPUT "${_CONFIG_FWD_STAMP}"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${GENSRC_DIR}/common"
    COMMAND "${Python3_EXECUTABLE}" "${_GEN_CONFIG_FWD_SCRIPT}"
            --output-dir "${GENSRC_DIR}/common"
    COMMAND "${CMAKE_COMMAND}" -E touch "${_CONFIG_FWD_STAMP}"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/config.h"
            "${_GEN_CONFIG_FWD_SCRIPT}"
    COMMENT "Generating config_*_fwd.h into ${GENSRC_DIR}/common/"
    VERBATIM
)
add_custom_target(GenerateConfigFwdHeaders DEPENDS "${_CONFIG_FWD_STAMP}")
add_dependencies(Common GenerateConfigFwdHeaders)
