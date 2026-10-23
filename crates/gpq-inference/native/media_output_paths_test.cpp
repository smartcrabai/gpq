#include "media.hpp"

#include <array>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {

struct TemporaryDirectory {
    std::filesystem::path path;

    ~TemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path, ignored);
    }
};

void require_output(bool success, const std::string& error) {
    if (!success) {
        throw std::runtime_error(error);
    }
}

}  // namespace

int main() {
    try {
        const auto suffix = std::chrono::steady_clock::now().time_since_epoch().count();
        TemporaryDirectory directory{
            std::filesystem::temp_directory_path() /
            std::filesystem::u8path(std::string(u8"gpq-media-\u753b\u50cf-\u52d5\u753b-") + std::to_string(suffix))};
        std::filesystem::create_directories(directory.path);

        std::array<unsigned char, 32 * 32 * 3> pixels{};
        sd_image_t frame{};
        frame.width = 32;
        frame.height = 32;
        frame.channel = 3;
        frame.data = pixels.data();

        std::string error;
        const auto png = directory.path / "image.png";
        const bool png_written = gpq::write_png(png, frame, error);
        require_output(png_written, error);
        if (!std::filesystem::is_regular_file(png)) {
            throw std::runtime_error("PNG output was not created in its Unicode directory");
        }

        std::array<float, 4'000> audio_samples{};
        sd_audio_t audio{};
        audio.data = audio_samples.data();
        audio.sample_rate = 48'000;
        audio.channels = 2;
        audio.sample_count = 2'000;
        const auto webm = directory.path / "video.webm";
        const bool webm_written = gpq::write_webm(webm, &frame, 1, 24, audio, error);
        require_output(webm_written, error);
        if (!std::filesystem::is_regular_file(webm)) {
            throw std::runtime_error("WebM output was not created in its Unicode directory");
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
