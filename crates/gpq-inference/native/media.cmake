include(FetchContent)

# Codec sources are pinned to upstream release commits. FetchContent keeps each
# source tree (and its upstream license files) in the native build directory.
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
if(MSVC)
    if(DEFINED CMAKE_MSVC_RUNTIME_LIBRARY)
        set(_gpq_media_saved_msvc_runtime "${CMAKE_MSVC_RUNTIME_LIBRARY}")
        set(_gpq_media_restore_msvc_runtime TRUE)
    else()
        set(_gpq_media_restore_msvc_runtime FALSE)
    endif()
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

set(WEBP_LINK_STATIC ON CACHE BOOL "" FORCE)
set(WEBP_BUILD_ANIM_UTILS OFF CACHE BOOL "" FORCE)
set(WEBP_BUILD_CWEBP OFF CACHE BOOL "" FORCE)
set(WEBP_BUILD_DWEBP OFF CACHE BOOL "" FORCE)
set(WEBP_BUILD_GIF2WEBP OFF CACHE BOOL "" FORCE)
set(WEBP_BUILD_IMG2WEBP OFF CACHE BOOL "" FORCE)
set(WEBP_BUILD_VWEBP OFF CACHE BOOL "" FORCE)
set(WEBP_BUILD_WEBPINFO OFF CACHE BOOL "" FORCE)
set(WEBP_BUILD_LIBWEBPMUX OFF CACHE BOOL "" FORCE)
set(WEBP_BUILD_WEBPMUX OFF CACHE BOOL "" FORCE)
set(WEBP_BUILD_EXTRAS OFF CACHE BOOL "" FORCE)
set(WEBP_BUILD_FUZZTEST OFF CACHE BOOL "" FORCE)
set(WEBP_USE_THREAD OFF CACHE BOOL "" FORCE)
FetchContent_Declare(gpq_media_webp
    GIT_REPOSITORY https://github.com/webmproject/libwebp.git
    GIT_TAG a4d7a715337ded4451fec90ff8ce79728e04126c
    GIT_SHALLOW FALSE
)
FetchContent_MakeAvailable(gpq_media_webp)

set(ENABLE_WEBMTS OFF CACHE BOOL "" FORCE)
set(ENABLE_WEBMINFO OFF CACHE BOOL "" FORCE)
set(ENABLE_TESTS OFF CACHE BOOL "" FORCE)
set(ENABLE_SAMPLE_PROGRAMS OFF CACHE BOOL "" FORCE)
set(ENABLE_WEBM_PARSER OFF CACHE BOOL "" FORCE)
set(ENABLE_WERROR OFF CACHE BOOL "" FORCE)
FetchContent_Declare(gpq_media_webm
    GIT_REPOSITORY https://github.com/webmproject/libwebm.git
    GIT_TAG 6745fd29e0245fc584b0bb9f65018ea2366fe7fb
    GIT_SHALLOW FALSE
)
FetchContent_MakeAvailable(gpq_media_webm)

# libwebm's pinned parser uses fixed-width integer types without including
# their standard header directly. The parser is a separate OBJECT target;
# applying the option to webm would only affect sources compiled by that target.
if(MSVC)
    target_compile_options(mkvparser PRIVATE "/FIcstdint")
else()
    target_compile_options(mkvparser PRIVATE "-include" "cstdint")
endif()

set(OPUS_BUILD_SHARED_LIBRARY OFF CACHE BOOL "" FORCE)
set(OPUS_BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(OPUS_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(OPUS_CUSTOM_MODES OFF CACHE BOOL "" FORCE)
set(OPUS_ENABLE_FLOAT_API ON CACHE BOOL "" FORCE)
set(OPUS_DRED OFF CACHE BOOL "" FORCE)
set(OPUS_OSCE OFF CACHE BOOL "" FORCE)
set(OPUS_INSTALL_PKG_CONFIG_MODULE OFF CACHE BOOL "" FORCE)
set(OPUS_INSTALL_CMAKE_CONFIG_MODULE OFF CACHE BOOL "" FORCE)
if(APPLE)
    set(OPUS_BUILD_FRAMEWORK OFF CACHE BOOL "" FORCE)
endif()
if(MSVC)
    set(OPUS_STATIC_RUNTIME ON CACHE BOOL "" FORCE)
endif()
FetchContent_Declare(gpq_media_opus
    GIT_REPOSITORY https://github.com/xiph/opus.git
    GIT_TAG ddbe48383984d56acd9e1ab6a090c54ca6b735a6
    GIT_SHALLOW FALSE
)
FetchContent_MakeAvailable(gpq_media_opus)

set(LIBSAMPLERATE_EXAMPLES OFF CACHE BOOL "" FORCE)
set(LIBSAMPLERATE_INSTALL OFF CACHE BOOL "" FORCE)
set(LIBSAMPLERATE_INSTALL_PKGCONFIG_MODULE OFF CACHE BOOL "" FORCE)
if(DEFINED BUILD_TESTING)
    set(_gpq_media_saved_build_testing "${BUILD_TESTING}")
    set(_gpq_media_restore_build_testing TRUE)
else()
    set(_gpq_media_restore_build_testing FALSE)
endif()
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
FetchContent_Declare(gpq_media_samplerate
    GIT_REPOSITORY https://github.com/libsndfile/libsamplerate.git
    GIT_TAG c96f5e3de9c4488f4e6c97f59f5245f22fda22f7
    GIT_SHALLOW FALSE
)
FetchContent_MakeAvailable(gpq_media_samplerate)
if(_gpq_media_restore_build_testing)
    set(BUILD_TESTING "${_gpq_media_saved_build_testing}")
else()
    unset(BUILD_TESTING)
endif()
unset(_gpq_media_saved_build_testing)
unset(_gpq_media_restore_build_testing)

# stb_image_write is used as a translation-unit-private PNG implementation
# (STB_IMAGE_WRITE_STATIC), so it exports no stb symbols into gpq_native.
FetchContent_Declare(gpq_media_stb
    GIT_REPOSITORY https://github.com/nothings/stb.git
    GIT_TAG a7d4c2175630e1ea5b6f8dd00ab96fed0b97b96d
    GIT_SHALLOW FALSE
)
FetchContent_GetProperties(gpq_media_stb)
if(NOT gpq_media_stb_POPULATED)
    FetchContent_Populate(gpq_media_stb)
endif()
if(MSVC)
    if(_gpq_media_restore_msvc_runtime)
        set(CMAKE_MSVC_RUNTIME_LIBRARY "${_gpq_media_saved_msvc_runtime}")
    else()
        unset(CMAKE_MSVC_RUNTIME_LIBRARY)
    endif()
    unset(_gpq_media_saved_msvc_runtime)
    unset(_gpq_media_restore_msvc_runtime)
endif()

add_library(gpq_media STATIC media.cpp)
target_compile_features(gpq_media PRIVATE cxx_std_17)
target_include_directories(gpq_media
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${stable_diffusion_cpp_SOURCE_DIR}/include"
    PRIVATE
        "${gpq_media_webm_SOURCE_DIR}"
        "${gpq_media_stb_SOURCE_DIR}"
)
target_link_libraries(gpq_media PUBLIC webp webm opus samplerate)
set_target_properties(gpq_media PROPERTIES POSITION_INDEPENDENT_CODE ON)
if(MSVC)
    set_property(TARGET gpq_media PROPERTY MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()
option(GPQ_BUILD_MEDIA_TESTS "Build native media output-path regression tests" OFF)
if(GPQ_BUILD_MEDIA_TESTS)
    enable_testing()
    add_executable(gpq_media_output_paths_test media_output_paths_test.cpp)
    target_link_libraries(gpq_media_output_paths_test PRIVATE gpq_media)
    add_test(NAME gpq_media_output_paths COMMAND gpq_media_output_paths_test)
endif()
