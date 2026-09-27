set(IRODS_TEST_TARGET irods_nanodbc_executor)

set(IRODS_TEST_SOURCE_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/src/test_nanodbc_executor.cpp
  ${CMAKE_SOURCE_DIR}/plugins/database/src/nanodbc_executor.cpp
  ${CMAKE_SOURCE_DIR}/plugins/database/src/database_session.cpp
  $<TARGET_OBJECTS:irods_genquery2_parser>
)

set(IRODS_TEST_INCLUDE_PATH
  ${CMAKE_SOURCE_DIR}/plugins/database/include
  ${CMAKE_SOURCE_DIR}/server/genquery2/include
  ${CMAKE_BINARY_DIR}/server/genquery2/dsl
  ${IRODS_EXTERNALS_FULLPATH_NANODBC}/include
  ${IRODS_EXTERNALS_FULLPATH_BOOST}/include
)

set(IRODS_TEST_LINK_LIBRARIES
  irods_common
  irods_server
  ${IRODS_EXTERNALS_FULLPATH_NANODBC}/lib/libnanodbc.so
  fmt::fmt
)
