set(IRODS_TEST_TARGET irods_collection_types)

set(IRODS_TEST_SOURCE_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/src/test_collection_types.cpp
  ${CMAKE_IRODS_SOURCE_DIR}/server/core/src/archive_engine.cpp
)

set(IRODS_TEST_INCLUDE_PATH
  ${IRODS_EXTERNALS_FULLPATH_BOOST}/include
  ${CMAKE_IRODS_SOURCE_DIR}/server/core/include
  ${LibArchive_INCLUDE_DIRS}
)

if (TARGET LibArchive::LibArchive)
  set(_IRODS_ARCHIVE_LIB LibArchive::LibArchive)
else()
  set(_IRODS_ARCHIVE_LIB "${LibArchive_LIBRARIES}")
endif()

set(IRODS_TEST_LINK_LIBRARIES
  irods_common
  irods_client
  fmt::fmt
  ${_IRODS_ARCHIVE_LIB}
)
