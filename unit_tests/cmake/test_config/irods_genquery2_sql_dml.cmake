set(IRODS_TEST_TARGET irods_genquery2_sql_dml)

set(IRODS_TEST_SOURCE_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/src/test_genquery2_sql_dml.cpp
  $<TARGET_OBJECTS:irods_genquery2_parser>
)

set(IRODS_TEST_INCLUDE_PATH
  ${CMAKE_SOURCE_DIR}/server/genquery2/include
  ${CMAKE_BINARY_DIR}/server/genquery2/dsl
  ${IRODS_EXTERNALS_FULLPATH_BOOST}/include
)

set(IRODS_TEST_LINK_LIBRARIES
  irods_common
  fmt::fmt
)
