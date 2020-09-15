find_package(OpenSSL REQUIRED)

set(
  IRODS_EXPERIMENTAL_API_PLUGIN_REMOVE_SOURCES
  ${CMAKE_SOURCE_DIR}/plugins/experimental/remove.cpp
  )

set(
  IRODS_EXPERIMENTAL_API_PLUGIN_REPLICATE_SOURCES
  ${CMAKE_SOURCE_DIR}/plugins/experimental/replicate.cpp
  )

set(
  IRODS_EXPERIMENTAL_API_PLUGIN_COPY_SOURCES
  ${CMAKE_SOURCE_DIR}/plugins/experimental/copy.cpp
  )

set(
  IRODS_EXPERIMENTAL_API_PLUGIN_REPLICATE_SOURCES
  ${CMAKE_SOURCE_DIR}/plugins/experimental/replicate.cpp
  )

set(
  IRODS_EXPERIMENTAL_API_PLUGIN_QUERY_SOURCES
  ${CMAKE_SOURCE_DIR}/plugins/experimental/genquery_parser_bison_generated.cpp
  ${CMAKE_SOURCE_DIR}/plugins/experimental/genquery_flex_output.cpp
  ${CMAKE_SOURCE_DIR}/plugins/experimental/genquery_wrapper.cpp
  ${CMAKE_SOURCE_DIR}/plugins/experimental/genquery_sql.cpp
  ${CMAKE_SOURCE_DIR}/plugins/experimental/query.cpp
  )

add_custom_command(
    OUTPUT ${CMAKE_SOURCE_DIR}/plugins/experimental/genquery_parser_bison_generated.cpp
    #INPUT  ${CMAKE_SOURCE_DIR}/plugins/experimental/genquery_bison_input.y
    COMMAND bison -o ${CMAKE_SOURCE_DIR}/plugins/experimental/genquery_parser_bison_generated.cpp ${CMAKE_SOURCE_DIR}/plugins/experimental/genquery_bison_input.y
    MAIN_DEPENDENCY ${CMAKE_SOURCE_DIR}/plugins/experimental/genquery_bison_input.y
)

add_custom_command(
    OUTPUT ${CMAKE_SOURCE_DIR}/plugins/experimental/genquery_flex_output.cpp
    #INPUT  ${CMAKE_SOURCE_DIR}/plugins/experimental/genquery_flex_input.l
    COMMAND flex -o ${CMAKE_SOURCE_DIR}/plugins/experimental/genquery_flex_output.cpp ${CMAKE_SOURCE_DIR}/plugins/experimental/genquery_flex_input.l
    MAIN_DEPENDENCY ${CMAKE_SOURCE_DIR}/plugins/experimental/genquery_flex_input.l
)

set(
  IRODS_EXPERIMENTAL_API_PLUGINS
  copy
  remove
  replicate
  query
  )

foreach(PLUGIN ${IRODS_EXPERIMENTAL_API_PLUGINS})
  string(TOUPPER ${PLUGIN} PLUGIN_UPPERCASE)

  add_library(
    ${PLUGIN}
    MODULE
    ${IRODS_EXPERIMENTAL_API_PLUGIN_${PLUGIN_UPPERCASE}_SOURCES}
    )

  target_include_directories(
    ${PLUGIN}
    PRIVATE
    ${CMAKE_BINARY_DIR}/lib/core/include
    ${CMAKE_SOURCE_DIR}/lib/filesystem/include
    ${CMAKE_SOURCE_DIR}/lib/api/include
    ${CMAKE_SOURCE_DIR}/lib/core/include
    ${CMAKE_SOURCE_DIR}/server/api/include
    ${CMAKE_SOURCE_DIR}/server/core/include
    ${CMAKE_SOURCE_DIR}/server/drivers/include
    ${CMAKE_SOURCE_DIR}/server/icat/include
    ${CMAKE_SOURCE_DIR}/server/re/include
    ${IRODS_EXTERNALS_FULLPATH_ARCHIVE}/include
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/include
    ${IRODS_EXTERNALS_FULLPATH_FMT}/include
    ${IRODS_EXTERNALS_FULLPATH_NANODBC}/include
    )
  target_link_libraries(
    ${PLUGIN}
    PRIVATE
    irods_server
    irods_common
    ${IRODS_EXTERNALS_FULLPATH_ARCHIVE}/lib/libarchive.so
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so
    ${IRODS_EXTERNALS_FULLPATH_FMT}/lib/libfmt.so
    ${IRODS_EXTERNALS_FULLPATH_NANODBC}/lib/libnanodbc.so
    ${OPENSSL_CRYPTO_LIBRARY}
    dl
    )

target_compile_definitions(${PLUGIN} PRIVATE ENABLE_RE ${IRODS_COMPILE_DEFINITIONS} BOOST_SYSTEM_NO_DEPRECATED IRODS_ENABLE_SYSLOG)
target_compile_options(${PLUGIN} PRIVATE -Wno-write-strings -Wno-sign-compare -Wno-ignored-qualifiers) # flex, nanodbc bugs
  set_property(TARGET ${PLUGIN} PROPERTY CXX_STANDARD ${IRODS_CXX_STANDARD})
endforeach()

install(
  TARGETS
  ${IRODS_EXPERIMENTAL_API_PLUGINS}
  LIBRARY
  DESTINATION ${IRODS_PLUGINS_DIRECTORY}/experimental
  COMPONENT ${IRODS_PACKAGE_COMPONENT_SERVER_NAME}
  )
