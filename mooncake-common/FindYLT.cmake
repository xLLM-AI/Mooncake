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
# Reproduce the canonical source tar hash from a checkout containing this pin:
# rev=54f9c0642386e7860e8e11ada698c484dc562d7d
# git -C /path/to/yalantinglibs archive --format=tar \
#   --prefix="yalantinglibs-${rev}/" "${rev}" | sha256sum
# The audited GitHub archive (not Gitee's HTML archive/download page) has gzip
# SHA256 11040b1d3f859599e5453ef7e9b5f84f8afa2388b057330f4c667973e5b20f01.
# `gzip -dc audited.tar.gz | sha256sum` gives the same tar hash below, including
# the yalantinglibs-<full-sha>/ prefix. Gzip encoders can differ across Git versions.
set(YLT_TAR_SHA256
    141d3020bbbb2f9fc2dc2a180f43e1fa17d03d986ab4c9fb06af09165e4e0710)
# GH_MIRROR / ASCEND_GITHUB_MIRROR_URLS rewrites GitHub archive URLs, not this
# Git source. Set -DYLT_GIT_REPOSITORY=<git-repository> to redirect YLT explicitly.
set(YLT_GIT_REPOSITORY
    "https://gitee.com/mirrors/yalantinglibs.git"
    CACHE STRING "Git mirror for the pinned yalantinglibs source")
option(YLT_ALLOW_UNVERIFIED_SOURCE
       "Allow YLT sources without Git metadata, bypassing source verification"
       OFF)
message(STATUS "YLT Git source: ${YLT_GIT_REPOSITORY} @ ${YLT_COMMIT}")
FetchContent_Declare(
  yalantinglibs
  GIT_REPOSITORY ${YLT_GIT_REPOSITORY}
  GIT_TAG ${YLT_COMMIT})

# Exclude from install for CMake before 3.28
FetchContent_GetProperties(yalantinglibs)
FetchContent_Populate(yalantinglibs)
# Validate Gitee's canonical source tar against the audited upstream payload,
# not just the Git commit pin.
# Every source without Git metadata requires an explicit, logged opt-out.
if(EXISTS "${yalantinglibs_SOURCE_DIR}/.git")
  find_package(Git REQUIRED)
  execute_process(
    COMMAND ${GIT_EXECUTABLE} -C ${yalantinglibs_SOURCE_DIR} rev-parse HEAD
    OUTPUT_VARIABLE YLT_SOURCE_COMMIT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE YLT_HEAD_RESULT)
  if(NOT YLT_HEAD_RESULT EQUAL 0 OR NOT YLT_SOURCE_COMMIT STREQUAL YLT_COMMIT)
    message(
      FATAL_ERROR "yalantinglibs checkout does not match the pinned commit")
  endif()
  # The archive hashes the commit, while CMake compiles the working tree.
  # Reject local edits and untracked source files that could shadow headers.
  execute_process(
    COMMAND ${GIT_EXECUTABLE} -C ${yalantinglibs_SOURCE_DIR} status --porcelain
            --untracked-files=all
    OUTPUT_VARIABLE YLT_SOURCE_STATUS
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE YLT_STATUS_RESULT)
  if(NOT YLT_STATUS_RESULT EQUAL 0 OR NOT YLT_SOURCE_STATUS STREQUAL "")
    message(FATAL_ERROR "yalantinglibs pinned checkout must be clean")
  endif()
  file(MAKE_DIRECTORY "${yalantinglibs_BINARY_DIR}")
  set(YLT_SOURCE_ARCHIVE "${yalantinglibs_BINARY_DIR}/pinned-source.tar")
  execute_process(
    COMMAND
      ${GIT_EXECUTABLE} -C ${yalantinglibs_SOURCE_DIR} archive --format=tar
      --prefix=yalantinglibs-${YLT_COMMIT}/ --output=${YLT_SOURCE_ARCHIVE}
      ${YLT_COMMIT}
    RESULT_VARIABLE YLT_ARCHIVE_RESULT)
  if(NOT YLT_ARCHIVE_RESULT EQUAL 0)
    message(FATAL_ERROR "Cannot verify the pinned yalantinglibs Git source")
  endif()
  file(SHA256 "${YLT_SOURCE_ARCHIVE}" YLT_ACTUAL_SHA256)
  if(NOT YLT_ACTUAL_SHA256 STREQUAL YLT_TAR_SHA256)
    message(FATAL_ERROR "yalantinglibs source archive SHA256 mismatch")
  endif()
  message(
    STATUS "YLT source verified: pinned HEAD, clean checkout and tar SHA256")
elseif(YLT_ALLOW_UNVERIFIED_SOURCE)
  message(
    WARNING
      "YLT source is UNVERIFIED: ${yalantinglibs_SOURCE_DIR} has no .git. "
      "HEAD, working-tree cleanliness and tar SHA256 checks were skipped by "
      "explicit YLT_ALLOW_UNVERIFIED_SOURCE=ON; expected pin ${YLT_COMMIT}.")
else()
  message(
    FATAL_ERROR
      "YLT source has no .git; cannot verify the pin, working tree or tar SHA256. "
      "Set -DYLT_ALLOW_UNVERIFIED_SOURCE=ON to opt out explicitly (with a warning)."
  )
endif()
add_subdirectory(${yalantinglibs_SOURCE_DIR} ${yalantinglibs_BINARY_DIR}
                 EXCLUDE_FROM_ALL)

# Suppress warnings for CMake before 3.25
get_target_property(YLT_INCL_DIRS yalantinglibs INTERFACE_INCLUDE_DIRECTORIES)
set_target_properties(
  yalantinglibs PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES
                           "${YLT_INCL_DIRS}")
