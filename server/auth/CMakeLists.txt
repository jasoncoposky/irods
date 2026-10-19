if (NOT PAM_LIBRARY)
  find_library(PAM_LIBRARY pam)
  if (PAM_LIBRARY)
    message(STATUS "Found pam: ${PAM_LIBRARY}")
  else()
    message(FATAL_ERROR "pam library not found")
  endif()
endif()

find_path(PAM_INCLUDE_DIR NAMES security/pam_appl.h)
if (PAM_INCLUDE_DIR)
  message(STATUS "Found pam include dir: ${PAM_INCLUDE_DIR}")
endif()

add_executable(
  irodsPamAuthCheck
  "${CMAKE_CURRENT_SOURCE_DIR}/src/irodsPamAuthCheck.cpp"
)
target_include_directories(
  irodsPamAuthCheck
  PRIVATE
  ${PAM_INCLUDE_DIR}
)
target_link_libraries(
  irodsPamAuthCheck
  PRIVATE
  "${PAM_LIBRARY}"
)

add_dependencies(all-server irodsPamAuthCheck)
install(
  TARGETS
  irodsPamAuthCheck
  RUNTIME
  DESTINATION "${CMAKE_INSTALL_SBINDIR}"
  COMPONENT ${IRODS_PACKAGE_COMPONENT_SERVER_NAME}
  PERMISSIONS SETUID OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
)
