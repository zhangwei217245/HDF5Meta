CONFIGURE_FILE(${PROJECT_SOURCE_DIR}/cmake/config.h.cmake ${PROJECT_SOURCE_DIR}/src/lib/include/config.h)

ADD_SUBDIRECTORY(lib)
ADD_SUBDIRECTORY(app)