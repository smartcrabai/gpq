#pragma once

#include "stable-diffusion.h"

#include <filesystem>
#include <string>

namespace gpq {

bool write_png(const std::filesystem::path& path, const sd_image_t& image, std::string& error);
// Consumes the generated interleaved stereo samples; sample_count is frames per channel.
bool write_webm(const std::filesystem::path& path, const sd_image_t* frames, int frame_count, int fps, const sd_audio_t& audio, std::string& error);

}  // namespace gpq
