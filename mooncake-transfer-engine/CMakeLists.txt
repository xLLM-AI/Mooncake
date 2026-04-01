cmake_minimum_required(VERSION 3.16)
project(mooncake-transfer-engine)

if (NOT GLOBAL_CONFIG)
  if (USE_ETCD)
    message(FATAL_ERROR "Cannot enable USE_ETCD while building transfer engine independently")
  endif()
  include(../mooncake-common/FindJsonCpp.cmake)
  include(../mooncake-common/FindGLOG.cmake)
  include(../mooncake-common/common.cmake)
endif() # GLOBAL_CONFIG

if (USE_ASCEND)
  include(../mooncake-common/FindMpi.cmake)
  file(GLOB ASCEND_TOOLKIT_ROOT "/usr/local/Ascend/ascend-toolkit/latest/*-linux")
  set(ASCEND_INCLUDE_DIR "${ASCEND_TOOLKIT_ROOT}/include")

  include_directories(/usr/local/include
                      /usr/include
                      ${ASCEND_INCLUDE_DIR}
                      ${ASCEND_INCLUDE_DIR}/hccl
                      ${ASCEND_INCLUDE_DIR}/experiment
                      ${ASCEND_INCLUDE_DIR}/experiment/hccl
                      ${ASCEND_INCLUDE_DIR}/experiment/slog/toolchain
                      ${ASCEND_INCLUDE_DIR}/experiment/metadef/common/util/error_manager
                      ${ASCEND_INCLUDE_DIR}/experiment/runtime
                      ${ASCEND_INCLUDE_DIR}/experiment/msprof
                      ${MPI_INCLUDE_DIR}
                      /usr/local/Ascend/ascend-toolkit/latest/tools/hccl_test/common/src/
                      )
endif()

if (USE_ASCEND_HETEROGENEOUS)
  file(GLOB ASCEND_TOOLKIT_ROOT "/usr/local/Ascend/ascend-toolkit/latest/*-linux")
  set(ASCEND_INCLUDE_DIR "${ASCEND_TOOLKIT_ROOT}/include")
  include_directories(/usr/local/include
                      /usr/include
                      ${ASCEND_INCLUDE_DIR})
endif()

if (USE_MLU)
  if (NOT DEFINED NEUWARE_ROOT OR NEUWARE_ROOT STREQUAL "")
    if (DEFINED ENV{NEUWARE_HOME} AND NOT "$ENV{NEUWARE_HOME}" STREQUAL "")
      set(NEUWARE_ROOT "$ENV{NEUWARE_HOME}")
    else()
      set(NEUWARE_ROOT "/usr/local/neuware")
    endif()
  endif()
  set(MLU_INCLUDE_DIR "${NEUWARE_ROOT}/include")
  set(MLU_LIB_DIR "${NEUWARE_ROOT}/lib64")
  include_directories(${MLU_INCLUDE_DIR})
endif()


include_directories(include)
add_subdirectory(include)
add_subdirectory(src)

if (BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()

if (BUILD_EXAMPLES)
  add_subdirectory(example)
endif()

if (WITH_RUST_EXAMPLE)
  add_subdirectory(rust)
endif()

if (USE_MNNVL)
  add_subdirectory(nvlink-allocator)
endif()
