set(IRODS_TEST_TARGET irods_zero_copy_io)

set(
  IRODS_TEST_SOURCE_FILES
  "${CMAKE_CURRENT_SOURCE_DIR}/src/test_zero_copy_io.cpp"
)

set(
  IRODS_TEST_INCLUDE_PATH
  "${CMAKE_IRODS_SOURCE_DIR}/lib/core/include"
  "${CMAKE_IRODS_BINARY_DIR}/lib/core/include"
)

set(
  IRODS_TEST_LINK_LIBRARIES
  irods_common
  irods_client
  fmt::fmt
)
