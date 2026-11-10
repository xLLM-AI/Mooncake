# Configure the real FindYLT module against isolated Git and non-Git sources.
# No network fetch or production source tree mutation is needed.
file(REMOVE_RECURSE "${TEST_BINARY_DIR}")
file(MAKE_DIRECTORY "${TEST_BINARY_DIR}/project")
file(
  WRITE "${TEST_BINARY_DIR}/project/CMakeLists.txt"
  "cmake_minimum_required(VERSION 3.20)\n"
  "project(ylt_source_probe LANGUAGES CXX)\n"
  "set(YLT_ENABLE_IBV OFF CACHE BOOL \"CPU probe\")\n"
  "set(BUILD_EXAMPLES OFF CACHE BOOL \"No dependency examples\")\n"
  "include(\"${MOONCAKE_SOURCE_DIR}/mooncake-common/FindYLT.cmake\")\n")
set(source "${TEST_BINARY_DIR}/git-source")
execute_process(
  COMMAND "${GIT_COMMAND}" clone --shared "${YLT_SOURCE_DIR}" "${source}"
  RESULT_VARIABLE clone_result
  OUTPUT_QUIET
  ERROR_VARIABLE clone_error)
if(NOT clone_result EQUAL 0)
  message(FATAL_ERROR "Cannot prepare isolated YLT source: ${clone_error}")
endif()
function(check_configure name expected_success expected_message)
  execute_process(
    COMMAND
      "${CMAKE_COMMAND}" -S "${TEST_BINARY_DIR}/project" -B
      "${TEST_BINARY_DIR}/${name}" "-DCMAKE_CXX_COMPILER=${CXX_COMPILER}"
      "-DGIT_EXECUTABLE=${GIT_COMMAND}" ${ARGN}
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error)
  file(WRITE "${TEST_BINARY_DIR}/${name}.log" "${output}\n${error}")
  if((expected_success AND NOT result EQUAL 0) OR (NOT expected_success
                                                   AND result EQUAL 0))
    message(
      FATAL_ERROR "${name}: unexpected result ${result}\n${output}\n${error}")
  endif()
  string(FIND "${output}\n${error}" "${expected_message}" message_position)
  if(message_position EQUAL -1)
    message(
      FATAL_ERROR "${name}: expected diagnostic missing\n${output}\n${error}")
  endif()
endfunction()
check_configure(clean TRUE "YLT source verified"
                "-DFETCHCONTENT_SOURCE_DIR_YALANTINGLIBS=${source}")
file(APPEND "${source}/include/ylt/struct_pack.hpp"
     "\n// test-only dirty header\n")
check_configure(
  dirty FALSE "pinned checkout must be clean"
  "-DFETCHCONTENT_SOURCE_DIR_YALANTINGLIBS=${source}"
  "-DYLT_ALLOW_UNVERIFIED_SOURCE=ON")
execute_process(
  COMMAND "${GIT_COMMAND}" -C "${source}" reset --hard HEAD
  RESULT_VARIABLE reset_result
  OUTPUT_QUIET)
if(NOT reset_result EQUAL 0)
  message(FATAL_ERROR "Cannot reset isolated YLT source")
endif()
# Removing only this disposable clone's Git metadata models an unpacked archive.
file(REMOVE_RECURSE "${source}/.git")
check_configure(explicit_non_git FALSE "YLT source has no .git"
                "-DFETCHCONTENT_SOURCE_DIR_YALANTINGLIBS=${source}")
check_configure(
  explicit_opt_out TRUE "YLT source is UNVERIFIED"
  "-DFETCHCONTENT_SOURCE_DIR_YALANTINGLIBS=${source}"
  "-DYLT_ALLOW_UNVERIFIED_SOURCE=ON")
# Model a non-Git cached _deps tree, without a SOURCE_DIR override.
file(MAKE_DIRECTORY "${TEST_BINARY_DIR}/implicit_non_git/_deps")
file(CREATE_LINK "${source}"
     "${TEST_BINARY_DIR}/implicit_non_git/_deps/yalantinglibs-src" SYMBOLIC)
check_configure(implicit_non_git FALSE "YLT source has no .git"
                "-DFETCHCONTENT_FULLY_DISCONNECTED=ON")
file(MAKE_DIRECTORY "${TEST_BINARY_DIR}/implicit_opt_out/_deps")
file(CREATE_LINK "${source}"
     "${TEST_BINARY_DIR}/implicit_opt_out/_deps/yalantinglibs-src" SYMBOLIC)
check_configure(
  implicit_opt_out TRUE "YLT source is UNVERIFIED"
  "-DFETCHCONTENT_FULLY_DISCONNECTED=ON" "-DYLT_ALLOW_UNVERIFIED_SOURCE=ON")
