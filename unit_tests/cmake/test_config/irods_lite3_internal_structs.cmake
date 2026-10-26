set(IRODS_TEST_TARGET irods_lite3_internal_structs)

set(
  IRODS_TEST_SOURCE_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/src/test_lite3_internal_structs.cpp
)

set(
  IRODS_TEST_INCLUDE_PATH
  ${CMAKE_BINARY_DIR}/lib/core/include
  ${CMAKE_SOURCE_DIR}/lib/core/include
  ${CMAKE_SOURCE_DIR}/lib/api/include
  ${CMAKE_SOURCE_DIR}/server/core/include
  ${CMAKE_SOURCE_DIR}/server/re/include
  ${CMAKE_SOURCE_DIR}/../lib/lite3-cpp/include
  ${CMAKE_SOURCE_DIR}/../lib/lite3-cpp/include/utils
)

set(
  IRODS_TEST_LINK_LIBRARIES
  irods_common
  irods_client
  irods_server
  fmt::fmt
)
