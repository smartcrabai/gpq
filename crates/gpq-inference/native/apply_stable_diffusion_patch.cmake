foreach(required_arg SOURCE_DIR PATCH_FILE PINNED_COMMIT PINNED_GGML_COMMIT GIT_EXECUTABLE)
    if(NOT DEFINED ${required_arg} OR "${${required_arg}}" STREQUAL "")
        message(FATAL_ERROR "apply_stable_diffusion_patch.cmake requires ${required_arg}")
    endif()
endforeach()
if(NOT EXISTS "${PATCH_FILE}")
    message(FATAL_ERROR "Pinned Vulkan LoRA CPY patch does not exist: ${PATCH_FILE}")
endif()
if(NOT IS_DIRECTORY "${SOURCE_DIR}")
    message(FATAL_ERROR "Pinned stable-diffusion.cpp source directory does not exist: ${SOURCE_DIR}")
endif()

function(gpq_require_source_diff expected_patch description)
    execute_process(
        COMMAND "${GIT_EXECUTABLE}" -C "${SOURCE_DIR}" diff --binary --no-ext-diff HEAD --
        RESULT_VARIABLE diff_result
        OUTPUT_VARIABLE source_diff
        ERROR_VARIABLE diff_error)
    if(NOT diff_result EQUAL 0)
        message(FATAL_ERROR "Could not inspect pinned stable-diffusion.cpp changes. ${diff_error}")
    endif()

    set(expected_diff "")
    if(NOT "${expected_patch}" STREQUAL "")
        file(READ "${expected_patch}" expected_diff)
    endif()
    string(REPLACE "\r\n" "\n" source_diff "${source_diff}")
    string(REPLACE "\r\n" "\n" expected_diff "${expected_diff}")
    if(NOT "${source_diff}" STREQUAL "${expected_diff}")
        message(FATAL_ERROR
            "${description}: pinned source changes are not exactly the desired patch or clean base; refusing to alter this cache. Use a fresh CMake build directory; this script will not reset the existing cache.")
    endif()

    execute_process(
        COMMAND "${GIT_EXECUTABLE}" -C "${SOURCE_DIR}" ls-files --others --exclude-standard
        RESULT_VARIABLE untracked_result
        OUTPUT_VARIABLE untracked_output
        ERROR_VARIABLE untracked_error)
    if(NOT untracked_result EQUAL 0 OR NOT "${untracked_output}" STREQUAL "")
        message(FATAL_ERROR
            "${description}: pinned source contains untracked files; refusing to alter this cache. Use a fresh CMake build directory; this script will not reset the existing cache. ${untracked_output}${untracked_error}")
    endif()

    execute_process(
        COMMAND "${GIT_EXECUTABLE}" -C "${SOURCE_DIR}/ggml" diff --quiet HEAD
        RESULT_VARIABLE ggml_diff_result
        ERROR_VARIABLE ggml_diff_error)
    if(NOT ggml_diff_result EQUAL 0)
        message(FATAL_ERROR
            "${description}: pinned GGML submodule contains tracked changes or could not be inspected; use a fresh CMake build directory. ${ggml_diff_error}")
    endif()

    execute_process(
        COMMAND "${GIT_EXECUTABLE}" -C "${SOURCE_DIR}/ggml" ls-files --others --exclude-standard
        RESULT_VARIABLE ggml_untracked_result
        OUTPUT_VARIABLE ggml_untracked_output
        ERROR_VARIABLE ggml_untracked_error)
    if(NOT ggml_untracked_result EQUAL 0 OR NOT "${ggml_untracked_output}" STREQUAL "")
        message(FATAL_ERROR
            "${description}: pinned GGML submodule contains untracked files; use a fresh CMake build directory. ${ggml_untracked_output}${ggml_untracked_error}")
    endif()
endfunction()
execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${SOURCE_DIR}" rev-parse HEAD
    RESULT_VARIABLE source_result
    OUTPUT_VARIABLE source_commit
    ERROR_VARIABLE source_error
    OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT source_result EQUAL 0 OR NOT "${source_commit}" STREQUAL "${PINNED_COMMIT}")
    message(FATAL_ERROR
        "stable-diffusion.cpp source mismatch: expected ${PINNED_COMMIT}, got '${source_commit}'. ${source_error}")
endif()

execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${SOURCE_DIR}/ggml" rev-parse HEAD
    RESULT_VARIABLE ggml_result
    OUTPUT_VARIABLE ggml_commit
    ERROR_VARIABLE ggml_error
    OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT ggml_result EQUAL 0 OR NOT "${ggml_commit}" STREQUAL "${PINNED_GGML_COMMIT}")
    message(FATAL_ERROR
        "stable-diffusion.cpp GGML submodule mismatch: expected ${PINNED_GGML_COMMIT}, got '${ggml_commit}'. ${ggml_error}")
endif()


execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${SOURCE_DIR}" apply --reverse --check "${PATCH_FILE}"
    RESULT_VARIABLE reverse_result
    OUTPUT_VARIABLE reverse_output
    ERROR_VARIABLE reverse_error)
if(reverse_result EQUAL 0)
    gpq_require_source_diff("${PATCH_FILE}" "The desired Vulkan LoRA CPY patch appears applied")
    return()
endif()


gpq_require_source_diff("" "Before applying the Vulkan LoRA CPY patch")

execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${SOURCE_DIR}" apply --check "${PATCH_FILE}"
    RESULT_VARIABLE check_result
    OUTPUT_VARIABLE check_output
    ERROR_VARIABLE check_error)
if(NOT check_result EQUAL 0)
    message(FATAL_ERROR
        "The pinned stable-diffusion.cpp source does not accept the Vulkan LoRA CPY patch. ${check_output}${check_error}")
endif()

execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${SOURCE_DIR}" apply "${PATCH_FILE}"
    RESULT_VARIABLE apply_result
    OUTPUT_VARIABLE apply_output
    ERROR_VARIABLE apply_error)
if(NOT apply_result EQUAL 0)
    message(FATAL_ERROR
        "Failed to apply the Vulkan LoRA CPY patch to pinned stable-diffusion.cpp. ${apply_output}${apply_error}")
endif()

execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${SOURCE_DIR}" apply --reverse --check "${PATCH_FILE}"
    RESULT_VARIABLE verify_result
    OUTPUT_VARIABLE verify_output
    ERROR_VARIABLE verify_error)
if(NOT verify_result EQUAL 0)
    message(FATAL_ERROR
        "The Vulkan LoRA CPY patch did not apply cleanly. ${verify_output}${verify_error}")
endif()
gpq_require_source_diff("${PATCH_FILE}" "After applying the Vulkan LoRA CPY patch")

