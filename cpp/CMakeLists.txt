cmake_minimum_required(VERSION 3.16)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    project(abstraction_watch LANGUAGES CXX VERSION 0.1.0)
    set(ABSTRACTION_WATCH_STANDALONE ON)
else()
    set(ABSTRACTION_WATCH_STANDALONE OFF)
endif()

include(GNUInstallDirs)
find_package(Threads REQUIRED)

add_library(abstraction_watch INTERFACE)
target_include_directories(abstraction_watch INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(abstraction_watch INTERFACE Threads::Threads)
target_compile_features(abstraction_watch INTERFACE cxx_std_17)
set_target_properties(abstraction_watch PROPERTIES EXPORT_NAME watch)
add_library(abstraction::watch ALIAS abstraction_watch)

# This layer's only C++ test reaches into the job layer, so it is built where
# both are present -- the root CMakeLists.txt -- and a standalone build of this
# directory has no test to run. Said here because a directory that configures,
# builds and reports zero tests otherwise reads as a pass.

if(ABSTRACTION_WATCH_STANDALONE)
    include(CMakePackageConfigHelpers)
    set(ABSTRACTION_WATCH_CMAKEDIR ${CMAKE_INSTALL_LIBDIR}/cmake/abstraction_watch)

    install(TARGETS abstraction_watch EXPORT abstraction_watchTargets)
    install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
    install(EXPORT abstraction_watchTargets
        FILE abstraction_watchTargets.cmake
        NAMESPACE abstraction::
        DESTINATION ${ABSTRACTION_WATCH_CMAKEDIR})

    configure_package_config_file(cmake/abstraction_watchConfig.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/abstraction_watchConfig.cmake
        INSTALL_DESTINATION ${ABSTRACTION_WATCH_CMAKEDIR})
    write_basic_package_version_file(
        ${CMAKE_CURRENT_BINARY_DIR}/abstraction_watchConfigVersion.cmake
        COMPATIBILITY SameMinorVersion)
    install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/abstraction_watchConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/abstraction_watchConfigVersion.cmake
        DESTINATION ${ABSTRACTION_WATCH_CMAKEDIR})
else()
    # A layer built as somebody else's subdirectory is theirs to install: the
    # top-level project reads these two properties to export the whole closure
    # it compiled, headers included, under its own package name.
    set_property(GLOBAL APPEND PROPERTY ABSTRACTION_SOURCE_TARGETS abstraction_watch)
    set_property(GLOBAL APPEND PROPERTY ABSTRACTION_SOURCE_INCLUDES
        ${CMAKE_CURRENT_SOURCE_DIR}/include/)
endif()
