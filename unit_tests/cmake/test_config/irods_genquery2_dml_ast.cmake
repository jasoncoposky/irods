set(IRODS_TEST_TARGET irods_genquery2_dml_ast)

set(IRODS_TEST_SOURCE_FILES ${CMAKE_CURRENT_SOURCE_DIR}/src/test_genquery2_dml_ast.cpp)

set(IRODS_TEST_INCLUDE_PATH
  ${CMAKE_SOURCE_DIR}/server/genquery2/include
  ${IRODS_EXTERNALS_FULLPATH_BOOST}/include
)

set(IRODS_TEST_LINK_LIBRARIES
  irods_common
)
