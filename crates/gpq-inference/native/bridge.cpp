#define STB_IMAGE_IMPLEMENTATION
#define STB_IMAGE_STATIC
#if defined(_WIN32)
#define STBI_WINDOWS_UTF8
#endif
#include <stb_image.h>

#include "stable-diffusion.h"
#include "media.hpp"
#include <json.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cstddef>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using json = nlohmann::json;
enum NativeStatus : uint32_t {
    NATIVE_STATUS_OK = 0,
    NATIVE_STATUS_INVALID_REQUEST = 1,
    NATIVE_STATUS_OUT_OF_MEMORY = 2,
    NATIVE_STATUS_CANCELLED = 3,
    NATIVE_STATUS_RUNTIME = 4,
};

struct CancelledError : std::runtime_error {
    using std::runtime_error::runtime_error;
};
using ProgressFn = void (*)(float, void*);
struct ProgressRegistrationData {
    ProgressFn callback;
    void* user_data;
};

std::mutex generation_mutex;
std::mutex active_context_mutex;
std::mutex log_mutex;
sd_ctx_t* active_context = nullptr;
std::atomic<bool> cancellation_requested{false};
std::atomic<bool> lora_application_warning{false};
std::atomic<bool> lora_application_active{false};
std::atomic<bool> lora_apply_mode_fallback{false};
std::atomic<bool> lora_requested{false};
std::atomic<bool> oom_reported{false};
std::array<char, 512> first_lora_warning{};
std::size_t first_lora_warning_size = 0;
void clear_lora_warning_detail() {
    std::lock_guard<std::mutex> lock(log_mutex);
    first_lora_warning_size = 0;
    first_lora_warning[0] = '\0';
}

std::string first_lora_warning_message() {
    std::lock_guard<std::mutex> lock(log_mutex);
    return std::string(first_lora_warning.data(), first_lora_warning_size);
}

bool has_nonzero_lora(const json& loras) {
    return std::any_of(loras.begin(), loras.end(), [](const json& lora) {
        return lora.at("scale").get<float>() != 0.0f;
    });
}

struct LoraApplicationPhase {
    LoraApplicationPhase() noexcept {
        lora_application_active.store(true, std::memory_order_relaxed);
    }

    ~LoraApplicationPhase() {
        lora_application_active.store(false, std::memory_order_relaxed);
    }
};


char* duplicate_string(const std::string& value) noexcept {
    char* result = static_cast<char*>(std::malloc(value.size() + 1));
    if (result == nullptr) {
        return nullptr;
    }
    std::memcpy(result, value.c_str(), value.size() + 1);
    return result;
}

void set_error(char** output, const std::string& message) noexcept {
    if (output != nullptr) {
        *output = duplicate_string(message);
    }
}
bool is_out_of_memory_message(const std::string& message) {
    std::string lowercase = message;
    std::transform(lowercase.begin(), lowercase.end(), lowercase.begin(), [](unsigned char character) {
        return static_cast<char>(std::tolower(character));
    });
    return lowercase.find("out of memory") != std::string::npos ||
           lowercase.find("out of device memory") != std::string::npos ||
           lowercase.find("out_of_device_memory") != std::string::npos ||
           lowercase.find("out_of_host_memory") != std::string::npos ||
           lowercase.find("outofmemory") != std::string::npos ||
           lowercase.find("failed to allocate") != std::string::npos ||
           lowercase.find("allocation failed") != std::string::npos ||
           lowercase.find("alloc failed") != std::string::npos ||
           lowercase.find("bad_alloc") != std::string::npos;
}

[[noreturn]] void throw_generation_failure(const char* message) {
    if (cancellation_requested.load(std::memory_order_relaxed)) {
        throw CancelledError("native generation was cancelled");
    }
    if (oom_reported.load(std::memory_order_relaxed)) {
        throw std::bad_alloc();
    }
    throw std::runtime_error(message);
}

[[noreturn]] void throw_media_failure(const char* prefix, const std::string& error) {
    if (oom_reported.load(std::memory_order_relaxed) || is_out_of_memory_message(error)) {
        throw std::bad_alloc();
    }
    throw std::runtime_error(std::string(prefix) + error);
}

void set_json(char** output, const json& value) {
    if (output == nullptr) {
        throw std::invalid_argument("native output pointer is null");
    }
    *output = duplicate_string(value.dump());
    if (*output == nullptr) {
        throw std::bad_alloc();
    }
}

std::string required_string(const json& object, const char* key) {
    const auto value = object.at(key).get<std::string>();
    if (value.empty() || value.find('\0') != std::string::npos) {
        throw std::invalid_argument(std::string("invalid empty or NUL-containing field: ") + key);
    }
    return value;
}

struct OwnedImage {
    sd_image_t view{};

    OwnedImage() = default;
    OwnedImage(const OwnedImage&) = delete;
    OwnedImage& operator=(const OwnedImage&) = delete;
    OwnedImage(OwnedImage&& other) noexcept : view(other.view) { other.view.data = nullptr; }
    OwnedImage& operator=(OwnedImage&& other) noexcept {
        if (this != &other) {
            stbi_image_free(view.data);
            view = other.view;
            other.view.data = nullptr;
        }
        return *this;
    }
    ~OwnedImage() { stbi_image_free(view.data); }
};

OwnedImage decode_image(const std::string& path) {
    int width = 0;
    int height = 0;
    int channels = 0;
    if (stbi_info(path.c_str(), &width, &height, &channels) == 0) {
        const char* failure = stbi_failure_reason();
        const std::string reason = failure == nullptr ? "unknown image decode error" : failure;
        if (is_out_of_memory_message(reason)) {
            throw std::bad_alloc();
        }
        throw std::invalid_argument("cannot inspect referenced image " + path + ": " + reason);
    }
    if (channels < 1 || channels > 4) {
        throw std::invalid_argument("referenced image has unsupported channels");
    }
    if (width <= 0 || height <= 0 || width > 16384 || height > 16384 ||
        static_cast<uint64_t>(width) * static_cast<uint64_t>(height) > 100000000ULL) {
        throw std::invalid_argument("referenced image dimensions exceed decode bounds");
    }
    int decoded_width = 0;
    int decoded_height = 0;
    int decoded_channels = 0;
    unsigned char* pixels = stbi_load(path.c_str(), &decoded_width, &decoded_height, &decoded_channels, 3);
    if (pixels == nullptr) {
        const char* failure = stbi_failure_reason();
        const std::string reason = failure == nullptr ? "unknown image decode error" : failure;
        if (is_out_of_memory_message(reason)) {
            throw std::bad_alloc();
        }
        throw std::invalid_argument("cannot decode referenced image " + path + ": " + reason);
    }
    if (decoded_width != width || decoded_height != height || decoded_width <= 0 || decoded_height <= 0 ||
        decoded_channels < 1 || decoded_channels > 4) {
        stbi_image_free(pixels);
        throw std::runtime_error("referenced image dimensions changed while decoding");
    }
    OwnedImage image;
    image.view.width = static_cast<uint32_t>(decoded_width);
    image.view.height = static_cast<uint32_t>(decoded_height);
    image.view.channel = 3;
    image.view.data = pixels;
    return image;
}

void validate_image(const sd_image_t& image, const char* role) {
    if (image.data == nullptr || image.width == 0 || image.height == 0 || image.channel < 1 || image.channel > 4 ||
        static_cast<uint64_t>(image.width) * image.height > 134217728ULL) {
        throw std::runtime_error(std::string("native runtime produced invalid ") + role);
    }
}

std::filesystem::path unique_output_path(const std::filesystem::path& directory, const std::string& filename) {
    const auto path = directory / filename;
    if (std::filesystem::exists(path)) {
        throw std::runtime_error("refusing to overwrite existing output " + path.string());
    }
    return path;
}

std::filesystem::path temporary_path(const std::filesystem::path& output) {
    auto temporary = output;
    temporary += ".partial";
    if (std::filesystem::exists(temporary)) {
        throw std::runtime_error("stale partial output exists: " + temporary.string());
    }
    return temporary;
}

void commit_output(const std::filesystem::path& temporary, const std::filesystem::path& output) {
    std::error_code error;
    std::filesystem::rename(temporary, output, error);
    if (error) {
        std::filesystem::remove(temporary);
        throw std::runtime_error("cannot commit completed output " + output.string() + ": " + error.message());
    }
}
struct PartialFile {
    explicit PartialFile(std::filesystem::path path) : path(std::move(path)) {}
    ~PartialFile() {
        if (!committed) {
            std::error_code error;
            std::filesystem::remove(path, error);
        }
    }
    void commit_to(const std::filesystem::path& destination) {
        commit_output(path, destination);
        committed = true;
    }
    std::filesystem::path path;
    bool committed = false;
};
struct GeneratedFiles {
    std::vector<std::filesystem::path> paths;
    bool committed = false;

    void track(std::filesystem::path path) { paths.push_back(std::move(path)); }
    ~GeneratedFiles() {
        if (!committed) {
            for (const auto& path : paths) {
                std::error_code error;
                std::filesystem::remove(path, error);
            }
        }
    }
};

void progress_callback(int step, int steps, float, void* data) noexcept {
    if (data == nullptr || steps <= 0) {
        return;
    }
    auto* registration = static_cast<ProgressRegistrationData*>(data);
    if (registration->callback != nullptr) {
        const float value = std::clamp(static_cast<float>(step) / static_cast<float>(steps), 0.0f, 1.0f);
        registration->callback(value, registration->user_data);
    }
}

void log_callback(enum sd_log_level_t level, const char* text, void*) noexcept {
    if (text == nullptr) {
        return;
    }
    try {
        std::string message(text);
        std::string lowered = message;
        std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char character) {
            return static_cast<char>(std::tolower(character));
        });
        if (level >= SD_LOG_WARN && is_out_of_memory_message(lowered)) {
            oom_reported.store(true, std::memory_order_relaxed);
        }
        const bool lora_warning_message = lora_requested.load(std::memory_order_relaxed) &&
                                          level >= SD_LOG_WARN &&
                                          lowered.find("lora") != std::string::npos;
        const bool lora_mode_fallback_message = lora_warning_message &&
                                                lowered.find("immediately lora apply mode") != std::string::npos &&
                                                lowered.find("at_runtime") != std::string::npos;
        if (lora_mode_fallback_message) {
            lora_apply_mode_fallback.store(true, std::memory_order_relaxed);
        }
        std::lock_guard<std::mutex> lock(log_mutex);
        if (lora_warning_message) {
            if (first_lora_warning_size == 0 || lora_mode_fallback_message) {
                first_lora_warning_size = std::min(message.size(), first_lora_warning.size() - 1);
                std::memcpy(first_lora_warning.data(), message.data(), first_lora_warning_size);
                first_lora_warning[first_lora_warning_size] = '\0';
            }
            if (lora_application_active.load(std::memory_order_relaxed)) {
                lora_application_warning.store(true, std::memory_order_relaxed);
            }
        }
        std::cerr << message << std::endl;
    } catch (...) {
    }
}

struct ActiveContext {
    explicit ActiveContext(sd_ctx_t* context) : context(context) {
        std::lock_guard<std::mutex> lock(active_context_mutex);
        if (active_context != nullptr) {
            throw std::runtime_error("another native context is already active");
        }
        active_context = context;
    }
    ~ActiveContext() {
        std::lock_guard<std::mutex> lock(active_context_mutex);
        if (active_context == context) {
            active_context = nullptr;
        }
    }
    sd_ctx_t* context;
};
struct ContextDeleter {
    void operator()(sd_ctx_t* context) const noexcept {
        if (context == nullptr) {
            return;
        }
        try {
            free_sd_ctx(context);
        } catch (const std::exception& exception) {
            std::cerr << "native context release failed: " << exception.what() << std::endl;
        } catch (...) {
            std::cerr << "native context release failed with an unknown exception" << std::endl;
        }
    }
};

struct ProgressRegistration {
    ProgressRegistrationData data;
    ProgressRegistration(ProgressFn callback, void* user_data) : data{callback, user_data} {
        sd_set_progress_callback(callback == nullptr ? nullptr : progress_callback, callback == nullptr ? nullptr : &data);
    }
    ~ProgressRegistration() { sd_set_progress_callback(nullptr, nullptr); }
};

struct ImageArray {
    sd_image_t* images = nullptr;
    int count = 0;
    ~ImageArray() {
        if (images != nullptr) {
            free_sd_images(images, count);
        }
    }
};

struct AudioOwner {
    sd_audio_t* audio = nullptr;
    ~AudioOwner() { free_sd_audio(audio); }
};


struct LoraSet {
    std::vector<sd_lora_t> values;

    void append(const json& source) {
        for (const auto& item : source) {
            const auto& path = item.at("path").get_ref<const std::string&>();
            const float scale = item.at("scale").get<float>();
            if (path.empty() || path.find('\0') != std::string::npos || !std::isfinite(scale)) {
                throw std::invalid_argument("LoRA path and scale are invalid");
            }
            if (scale == 0.0f) {
                continue;
            }
            values.push_back(sd_lora_t{false, scale, path.c_str()});
        }
    }
};

struct SampleParamsOwner {
    sd_sample_params_t value{};
    std::vector<float> sigmas;
};

void populate_sample_params(SampleParamsOwner& owner, const json& model, sd_ctx_t* context) {
    auto& parameters = owner.value;
    const auto steps = model.at("steps").get<uint32_t>();
    if (steps == 0 || steps > static_cast<uint32_t>(std::numeric_limits<int>::max())) {
        throw std::invalid_argument("sampling steps exceed the native API range");
    }
    parameters.sample_steps = static_cast<int>(steps);
    parameters.guidance.txt_cfg = model.at("cfg_scale").get<float>();
    if (!std::isfinite(parameters.guidance.txt_cfg)) {
        throw std::invalid_argument("cfg_scale must be finite");
    }
    parameters.sample_method = sd_get_default_sample_method(context);
    if (parameters.sample_method >= SAMPLE_METHOD_COUNT) {
        throw std::runtime_error("native model has no supported default sampler");
    }
    parameters.scheduler = sd_get_default_scheduler(context, parameters.sample_method);
    if (parameters.scheduler >= SCHEDULER_COUNT) {
        throw std::runtime_error("native model has no supported default scheduler");
    }
    owner.sigmas = model.at("sigmas").get<std::vector<float>>();
    if (!owner.sigmas.empty()) {
        if (owner.sigmas.size() != static_cast<size_t>(steps) + 1 ||
            owner.sigmas.size() > static_cast<size_t>(std::numeric_limits<int>::max()) ||
            std::any_of(owner.sigmas.begin(), owner.sigmas.end(), [](float sigma) { return !std::isfinite(sigma); })) {
            throw std::invalid_argument("custom sigma schedule does not match sampling steps");
        }
        parameters.custom_sigmas = owner.sigmas.data();
        parameters.custom_sigmas_count = static_cast<int>(owner.sigmas.size());
    }
}

std::string_view trim_view(std::string_view value) {
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
        value.remove_prefix(1);
    }
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) {
        value.remove_suffix(1);
    }
    return value;
}

bool equals_case_insensitive(std::string_view left, std::string_view right) {
    return left.size() == right.size() &&
           std::equal(left.begin(), left.end(), right.begin(), [](char lhs, char rhs) {
               return std::tolower(static_cast<unsigned char>(lhs)) ==
                      std::tolower(static_cast<unsigned char>(rhs));
           });
}

std::string_view diffusion_backend_from_assignment(std::string_view specification) {
    std::string_view default_backend;
    std::string_view diffusion_backend;
    while (!specification.empty()) {
        const size_t comma = specification.find(',');
        const std::string_view part = trim_view(specification.substr(0, comma));
        if (comma == std::string_view::npos) {
            specification = {};
        } else {
            specification.remove_prefix(comma + 1);
        }
        if (part.empty()) {
            continue;
        }

        const size_t equals = part.find('=');
        if (equals == std::string_view::npos) {
            default_backend = part;
            continue;
        }

        const std::string_view key = trim_view(part.substr(0, equals));
        const std::string_view value = trim_view(part.substr(equals + 1));
        if (equals_case_insensitive(key, "all") ||
            equals_case_insensitive(key, "default") ||
            key == "*") {
            default_backend = value;
        } else if (equals_case_insensitive(key, "diffusion") ||
                   equals_case_insensitive(key, "model") ||
                   equals_case_insensitive(key, "unet") ||
                   equals_case_insensitive(key, "dit")) {
            diffusion_backend = value;
        }
    }
    return diffusion_backend.empty() ? default_backend : diffusion_backend;
}

bool has_vulkan_prefix(std::string_view backend) {
    return backend.size() >= 6 &&
           equals_case_insensitive(backend.substr(0, 6), "vulkan");
}

std::string backend_from_request(const json& runtime) {
    const auto backend = required_string(runtime, "backend");
    if (backend.find('\0') != std::string::npos) {
        throw std::invalid_argument("backend contains a NUL byte");
    }
    return backend;
}

void make_context_params(const json& request,
                         std::string& params_backend,
                         std::string& max_vram,
                         std::string& model_args,
                         std::string& diffusion_model,
                         std::string& text_encoder,
                         std::string& vision_encoder,
                         std::string& vae,
                         std::string& audio_vae) {
    const auto& model = request.at("model");
    const auto& runtime = request.at("runtime");
    diffusion_model = required_string(model, "diffusion_model");
    text_encoder = required_string(model, "text_encoder");
    vision_encoder = model.at("vision_encoder").is_null() ? std::string() : model.at("vision_encoder").get<std::string>();
    vae = required_string(model, "vae");
    audio_vae = model.at("audio_vae").is_null() ? std::string() : model.at("audio_vae").get<std::string>();
    params_backend = runtime.at("params_backend").is_null() ? std::string() : runtime.at("params_backend").get<std::string>();
    max_vram = runtime.at("max_vram").is_null() ? std::string() : runtime.at("max_vram").get<std::string>();
    model_args = runtime.at("model_args").get<std::string>();
}

json write_image_outputs(const json& request, sd_ctx_t* context, const std::filesystem::path& output_directory, GeneratedFiles& files) {
    const auto& generation = request.at("generation");
    const auto& model = request.at("model");
    std::vector<OwnedImage> owned_references;
    for (const auto& path : generation.at("references")) {
        owned_references.push_back(decode_image(path.get_ref<const std::string&>()));
    }
    std::vector<sd_image_t> references;
    references.reserve(owned_references.size());
    for (const auto& image : owned_references) {
        references.push_back(image.view);
    }
    if (!owned_references.empty() && !sd_ctx_supports_ref_image_conditioning(context)) {
        throw std::runtime_error("loaded model does not support reference-image conditioning");
    }

    LoraSet loras;
    loras.append(model.at("default_loras"));
    loras.append(generation.at("loras"));

    sd_img_gen_params_t parameters{};
    sd_img_gen_params_init(&parameters);
    parameters.width = static_cast<int>(generation.at("width").get<uint32_t>());
    parameters.height = static_cast<int>(generation.at("height").get<uint32_t>());
    parameters.seed = request.at("seed").get<int64_t>();
    parameters.loras = loras.values.empty() ? nullptr : loras.values.data();
    parameters.lora_count = static_cast<uint32_t>(loras.values.size());
    parameters.ref_images = references.empty() ? nullptr : references.data();
    parameters.ref_images_count = static_cast<int>(references.size());
    parameters.vae_tiling_params.enabled = request.at("runtime").at("vae_tiling").get<bool>();

    const auto& prompt = generation.at("prompt").get_ref<const std::string&>();
    const auto& negative_prompt = generation.at("negative_prompt").get_ref<const std::string&>();
    parameters.prompt = prompt.c_str();
    parameters.negative_prompt = negative_prompt.c_str();

    SampleParamsOwner sample_params;
    sd_sample_params_init(&sample_params.value);
    populate_sample_params(sample_params, model, context);
    parameters.sample_params = sample_params.value;

    ImageArray result;
    if (!generate_image(context, &parameters, &result.images, &result.count) || result.count <= 0 || result.images == nullptr) {
        throw_generation_failure("stable-diffusion.cpp image generation failed");
    }
    json outputs = json::array();
    for (int index = 0; index < result.count; ++index) {
        const sd_image_t& image = result.images[index];
        validate_image(image, "image");
        if (image.width != generation.at("width").get<uint32_t>() ||
            image.height != generation.at("height").get<uint32_t>()) {
            throw std::runtime_error("native image dimensions do not match the requested size");
        }
        const std::string filename = "image-" + std::to_string(index) + ".png";
        const std::filesystem::path destination = unique_output_path(output_directory, filename);
        files.track(destination);
        const std::filesystem::path temporary = temporary_path(destination);
        PartialFile temporary_file(temporary);
        std::string error;
        if (!gpq::write_png(temporary, image, error)) {
            throw_media_failure("PNG encoding failed: ", error);
        }
        temporary_file.commit_to(destination);
        outputs.push_back({{"path", destination.u8string()},
                           {"mime_type", "image/png"},
                           {"kind", "image"},
                           {"width", image.width},
                           {"height", image.height},
                           {"frames", nullptr},
                           {"duration_seconds", nullptr}});
    }
    return outputs;
}

json write_video_output(const json& request, sd_ctx_t* context, const std::filesystem::path& output_directory, GeneratedFiles& files) {
    const auto& generation = request.at("generation");
    const auto& model = request.at("model");
    std::optional<OwnedImage> first_frame;
    std::optional<OwnedImage> last_frame;
    if (!generation.at("first_frame").is_null()) {
        first_frame.emplace(decode_image(generation.at("first_frame").get_ref<const std::string&>()));
    }
    if (!generation.at("last_frame").is_null()) {
        if (!first_frame.has_value()) {
            throw std::invalid_argument("last-frame conditioning requires a first frame");
        }
        last_frame.emplace(decode_image(generation.at("last_frame").get_ref<const std::string&>()));
    }
    if (request.at("model").at("audio_vae").is_null()) {
        throw std::invalid_argument("audio-video generation requires an audio VAE");
    }
    if (generation.at("frames").get<uint32_t>() < 5 ||
        (generation.at("frames").get<uint32_t>() - 5) % 17 != 0) {
        throw std::invalid_argument("MiniMax-H3 frame count must be 17k+5 and at least five");
    }

    LoraSet loras;
    loras.append(model.at("default_loras"));

    sd_vid_gen_params_t parameters{};
    sd_vid_gen_params_init(&parameters);
    const auto& prompt = generation.at("prompt").get_ref<const std::string&>();
    const auto& negative_prompt = generation.at("negative_prompt").get_ref<const std::string&>();
    parameters.prompt = prompt.c_str();
    parameters.negative_prompt = negative_prompt.c_str();
    parameters.width = static_cast<int>(generation.at("width").get<uint32_t>());
    parameters.height = static_cast<int>(generation.at("height").get<uint32_t>());
    parameters.video_frames = static_cast<int>(generation.at("frames").get<uint32_t>());
    parameters.fps = 24;
    parameters.seed = request.at("seed").get<int64_t>();
    parameters.loras = loras.values.empty() ? nullptr : loras.values.data();
    parameters.lora_count = static_cast<uint32_t>(loras.values.size());
    if (first_frame.has_value()) {
        parameters.init_image = first_frame->view;
    }
    if (last_frame.has_value()) {
        parameters.end_image = last_frame->view;
    }
    parameters.vae_tiling_params.enabled = request.at("runtime").at("vae_tiling").get<bool>();

    SampleParamsOwner sample_params;
    sd_sample_params_init(&sample_params.value);
    populate_sample_params(sample_params, model, context);
    parameters.sample_params = sample_params.value;

    ImageArray frames;
    AudioOwner audio;
    int fps = 0;
    if (!generate_video(context, &parameters, &frames.images, &frames.count, &audio.audio, &fps) ||
        frames.images == nullptr || frames.count <= 0) {
        throw_generation_failure("stable-diffusion.cpp video generation failed");
    }
    if (frames.count != static_cast<int>(generation.at("frames").get<uint32_t>()) || fps != 24) {
        throw std::runtime_error("native video dimensions or frame timing differ from the H3 request");
    }
    if (audio.audio == nullptr || audio.audio->data == nullptr || audio.audio->sample_rate == 0 ||
        audio.audio->channels == 0 || audio.audio->sample_count == 0 || fps <= 0) {
        throw std::runtime_error("MiniMax-H3 did not return valid synchronized audio");
    }
    for (int index = 0; index < frames.count; ++index) {
        validate_image(frames.images[index], "video frame");
        if (frames.images[index].width != frames.images[0].width ||
            frames.images[index].height != frames.images[0].height) {
            throw std::runtime_error("native video frames have inconsistent dimensions");
        }
        if (frames.images[index].width != generation.at("width").get<uint32_t>() ||
            frames.images[index].height != generation.at("height").get<uint32_t>()) {
            throw std::runtime_error("native video dimensions do not match the requested size");
        }
    }
    const std::string filename = "video.webm";
    const std::filesystem::path destination = unique_output_path(output_directory, filename);
    files.track(destination);
    const std::filesystem::path temporary = temporary_path(destination);
    PartialFile temporary_file(temporary);
    std::string error;
    if (!gpq::write_webm(temporary, frames.images, frames.count, fps, *audio.audio, error)) {
        throw_media_failure("WebM encoding failed: ", error);
    }
    temporary_file.commit_to(destination);
    const double duration = static_cast<double>(frames.count) / fps;
    return json::array({{{"path", destination.u8string()},
                         {"mime_type", "video/webm"},
                         {"kind", "video"},
                         {"width", frames.images[0].width},
                         {"height", frames.images[0].height},
                         {"frames", frames.count},
                         {"duration_seconds", duration}}});
}

} // namespace

extern "C" uint32_t gpq_native_generate(const char* request_json,
                                    ProgressFn progress,
                                    void* progress_data,
                                    char** output_json,
                                    char** error) noexcept {
    if (output_json != nullptr) {
        *output_json = nullptr;
    }
    if (error != nullptr) {
        *error = nullptr;
    }
    try {
        if (request_json == nullptr || output_json == nullptr || error == nullptr) {
            throw std::invalid_argument("invalid native generation arguments");
        }
        std::lock_guard<std::mutex> generation_lock(generation_mutex);
        cancellation_requested.store(false, std::memory_order_relaxed);
        lora_application_warning.store(false, std::memory_order_relaxed);
        lora_application_active.store(false, std::memory_order_relaxed);
        lora_apply_mode_fallback.store(false, std::memory_order_relaxed);
        lora_requested.store(false, std::memory_order_relaxed);
        oom_reported.store(false, std::memory_order_relaxed);
        clear_lora_warning_detail();
        sd_set_log_callback(log_callback, nullptr);
        const json request = json::parse(request_json);
        const auto& default_loras = request.at("model").at("default_loras");
        const auto& generation = request.at("generation");
        const bool has_active_request_lora =
            has_nonzero_lora(default_loras) ||
            (generation.contains("loras") && has_nonzero_lora(generation.at("loras")));
        lora_requested.store(has_active_request_lora, std::memory_order_relaxed);
        const std::string family = required_string(request, "family");
        const auto& runtime = request.at("runtime");
        const std::string backend = backend_from_request(runtime);
        std::string params_backend;
        std::string max_vram;
        std::string model_args;
        std::string diffusion_model;
        std::string text_encoder;
        std::string vision_encoder;
        std::string vae;
        std::string audio_vae;
        make_context_params(request, params_backend, max_vram, model_args,
                            diffusion_model, text_encoder, vision_encoder, vae, audio_vae);

        sd_ctx_params_t context_parameters{};
        sd_ctx_params_init(&context_parameters);
        const std::string_view diffusion_backend = diffusion_backend_from_assignment(backend);
        const bool immediate_lora_mode_requested =
            has_vulkan_prefix(diffusion_backend) && lora_requested.load(std::memory_order_relaxed);
        if (immediate_lora_mode_requested) {
            context_parameters.lora_apply_mode = LORA_APPLY_IMMEDIATELY;
        }
        context_parameters.diffusion_model_path = diffusion_model.c_str();
        context_parameters.llm_path = text_encoder.c_str();
        context_parameters.llm_vision_path = vision_encoder.empty() ? nullptr : vision_encoder.c_str();
        context_parameters.vae_path = vae.c_str();
        context_parameters.audio_vae_path = audio_vae.empty() ? nullptr : audio_vae.c_str();
        const uint32_t threads = runtime.at("threads").get<uint32_t>();
        if (threads == 0 || threads > static_cast<uint32_t>(std::numeric_limits<int>::max())) {
            throw std::invalid_argument("thread count exceeds the native API range");
        }
        context_parameters.n_threads = static_cast<int>(threads);
        context_parameters.enable_mmap = true;
        context_parameters.backend = backend.c_str();
        context_parameters.params_backend = params_backend.empty() ? nullptr : params_backend.c_str();
        context_parameters.max_vram = max_vram.empty() ? nullptr : max_vram.c_str();
        context_parameters.model_args = model_args.empty() ? nullptr : model_args.c_str();

        std::unique_ptr<sd_ctx_t, ContextDeleter> context(new_sd_ctx(&context_parameters), ContextDeleter{});
        if (immediate_lora_mode_requested &&
            lora_apply_mode_fallback.load(std::memory_order_relaxed)) {
            throw std::runtime_error("stable-diffusion.cpp could not honor immediate Vulkan LoRA mode: " +
                                     first_lora_warning_message());
        }
        if (!context) {
            throw_generation_failure("stable-diffusion.cpp could not load the selected model and backend");
        }
        ActiveContext active(context.get());
        ProgressRegistration progress_registration(progress, progress_data);
        clear_lora_warning_detail();
        lora_application_warning.store(false, std::memory_order_relaxed);
        LoraApplicationPhase lora_application_phase;

        GeneratedFiles generated_files;
        const std::filesystem::path output_directory = std::filesystem::u8path(required_string(request, "output_dir"));
        json outputs;
        if (family == "qwen_image_2_1") {
            if (!sd_ctx_supports_image_generation(context.get())) {
                throw std::runtime_error("loaded model does not support image generation");
            }
            outputs = write_image_outputs(request, context.get(), output_directory, generated_files);
        } else if (family == "minimax_h3") {
            if (!sd_ctx_supports_video_generation(context.get())) {
                throw std::runtime_error("loaded model does not support audio-video generation");
            }
            outputs = write_video_output(request, context.get(), output_directory, generated_files);
        } else {
            throw std::invalid_argument("unsupported pinned model family");
        }
        if (lora_application_warning.load(std::memory_order_relaxed)) {
            throw std::runtime_error("stable-diffusion.cpp reported a LoRA load/application warning: " +
                                     first_lora_warning_message());
        }
        if (cancellation_requested.load(std::memory_order_relaxed)) {
            throw CancelledError("native generation was cancelled");
        }
        set_json(output_json, outputs);
        generated_files.committed = true;
        return NATIVE_STATUS_OK;
    } catch (const std::bad_alloc& exception) {
        set_error(error, exception.what());
        return NATIVE_STATUS_OUT_OF_MEMORY;
    } catch (const CancelledError& exception) {
        set_error(error, exception.what());
        return NATIVE_STATUS_CANCELLED;
    } catch (const json::exception& exception) {
        set_error(error, exception.what());
        return NATIVE_STATUS_INVALID_REQUEST;
    } catch (const std::invalid_argument& exception) {
        set_error(error, exception.what());
        return NATIVE_STATUS_INVALID_REQUEST;
    } catch (const std::exception& exception) {
        set_error(error, exception.what());
        return oom_reported.load(std::memory_order_relaxed)
                   ? NATIVE_STATUS_OUT_OF_MEMORY
                   : NATIVE_STATUS_RUNTIME;
    } catch (...) {
        set_error(error, "unknown exception in native inference bridge");
        return NATIVE_STATUS_RUNTIME;
    }
}

extern "C" uint32_t gpq_native_list_devices(char** output_json, char** error) noexcept {
    if (output_json != nullptr) {
        *output_json = nullptr;
    }
    if (error != nullptr) {
        *error = nullptr;
    }
    try {
        if (output_json == nullptr || error == nullptr) {
            throw std::invalid_argument("invalid device-list arguments");
        }
        const size_t required = sd_list_devices(nullptr, 0);
        if (required == 0) {
            throw std::runtime_error("native runtime reported no backend devices");
        }
        std::vector<char> buffer(required + 1, '\0');
        const size_t written = sd_list_devices(buffer.data(), buffer.size());
        if (written > required) {
            throw std::runtime_error("native device listing changed while being read");
        }
        const std::string listing(buffer.data(), written);
        json devices = json::array();
        size_t start = 0;
        while (start < listing.size()) {
            const size_t end = listing.find('\n', start);
            const std::string line = listing.substr(start, end == std::string::npos ? std::string::npos : end - start);
            if (!line.empty()) {
                const size_t tab = line.find('\t');
                if (tab == std::string::npos || tab == 0) {
                    throw std::runtime_error("native device entry has no name/description separator");
                }
                devices.push_back({{"name", line.substr(0, tab)}, {"description", line.substr(tab + 1)}});
            }
            if (end == std::string::npos) {
                break;
            }
            start = end + 1;
        }
        set_json(output_json, devices);
        return NATIVE_STATUS_OK;
    } catch (const std::bad_alloc& exception) {
        set_error(error, exception.what());
        return NATIVE_STATUS_OUT_OF_MEMORY;
    } catch (const std::invalid_argument& exception) {
        set_error(error, exception.what());
        return NATIVE_STATUS_INVALID_REQUEST;
    } catch (const std::exception& exception) {
        set_error(error, exception.what());
        return NATIVE_STATUS_RUNTIME;
    } catch (...) {
        set_error(error, "unknown exception while listing native devices");
        return NATIVE_STATUS_RUNTIME;
    }
}

extern "C" bool gpq_native_cancel_current() noexcept {
    try {
        std::lock_guard<std::mutex> lock(active_context_mutex);
        if (active_context == nullptr) {
            return false;
        }
        cancellation_requested.store(true, std::memory_order_relaxed);
        sd_cancel_generation(active_context, SD_CANCEL_ALL);
        return true;
    } catch (...) {
        return false;
    }
}

extern "C" void gpq_native_free_string(char* value) noexcept { std::free(value); }
