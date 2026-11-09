include_guard(GLOBAL)

include(FetchContent)

if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

if(APPLE)
  # macOS has no usable ibverbs; build yalantinglibs RPC over TCP only.
  set(YLT_ENABLE_IBV
      OFF
      CACHE BOOL "Enable yalantinglibs ibverbs support")
elseif(USE_SHCA)
  # SHCA redefines IB LID as u17, which stock yalanting ib_socket.hpp cannot
  # compile against. Control-plane RPC stays on TCP here; data-plane RDMA goes
  # through Transfer Engine / SHCA, not yalanting ibverbs.
  set(YLT_ENABLE_IBV
      OFF
      CACHE BOOL "Enable yalantinglibs ibverbs support" FORCE)
  message(STATUS "USE_SHCA=ON: forcing YLT_ENABLE_IBV=OFF")
else()
  set(YLT_ENABLE_IBV
      ON
      CACHE BOOL "Enable yalantinglibs ibverbs support")
endif()

set(YLT_COMMIT 54f9c0642386e7860e8e11ada698c484dc562d7d)
set(YLT_ARCHIVE_SHA256
    11040b1d3f859599e5453ef7e9b5f84f8afa2388b057330f4c667973e5b20f01)
set(YLT_GIT_REPOSITORY
    "https://gitee.com/mirrors/yalantinglibs.git"
    CACHE STRING "Git mirror for the pinned yalantinglibs source")
FetchContent_Declare(
  yalantinglibs
  GIT_REPOSITORY ${YLT_GIT_REPOSITORY}
  GIT_TAG ${YLT_COMMIT})

# Exclude from install for CMake before 3.28
FetchContent_GetProperties(yalantinglibs)
FetchContent_Populate(yalantinglibs)
# A canonical archive exported from Gitee is byte-identical to the audited
# upstream archive. Keep its SHA256 validation, not just the Git commit pin.
# Explicit non-Git SOURCE_DIR overrides retain FetchContent's existing bypass.
if(EXISTS "${yalantinglibs_SOURCE_DIR}/.git")
  find_package(Git REQUIRED)
  file(MAKE_DIRECTORY "${yalantinglibs_BINARY_DIR}")
  set(YLT_SOURCE_ARCHIVE "${yalantinglibs_BINARY_DIR}/pinned-source.tar.gz")
  execute_process(
    COMMAND
      ${GIT_EXECUTABLE} -C ${yalantinglibs_SOURCE_DIR} archive --format=tar.gz
      --prefix=yalantinglibs-${YLT_COMMIT}/ --output=${YLT_SOURCE_ARCHIVE}
      ${YLT_COMMIT}
    RESULT_VARIABLE YLT_ARCHIVE_RESULT)
  if(NOT YLT_ARCHIVE_RESULT EQUAL 0)
    message(FATAL_ERROR "Cannot verify the pinned yalantinglibs Git source")
  endif()
  file(SHA256 "${YLT_SOURCE_ARCHIVE}" YLT_ACTUAL_SHA256)
  if(NOT YLT_ACTUAL_SHA256 STREQUAL YLT_ARCHIVE_SHA256)
    message(FATAL_ERROR "yalantinglibs source archive SHA256 mismatch")
  endif()
endif()
add_subdirectory(${yalantinglibs_SOURCE_DIR} ${yalantinglibs_BINARY_DIR}
                 EXCLUDE_FROM_ALL)

# Suppress warnings for CMake before 3.25
get_target_property(YLT_INCL_DIRS yalantinglibs INTERFACE_INCLUDE_DIRECTORIES)
set_target_properties(
  yalantinglibs PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES
                           "${YLT_INCL_DIRS}")
