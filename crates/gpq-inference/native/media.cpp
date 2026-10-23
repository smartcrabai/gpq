#define STB_IMAGE_WRITE_STATIC
#define STB_IMAGE_WRITE_IMPLEMENTATION
#include "stb_image_write.h"

#include "media.hpp"

#include <mkvmuxer/mkvmuxer.h>
#include <opus.h>
#include <samplerate.h>
#include <webp/encode.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <sys/types.h>
#endif

namespace gpq {
namespace {

constexpr uint32_t kOpusSampleRate = 48000;
constexpr int kOpusChannels = 2;
constexpr int kOpusFrameSamples = 960;  // 20 ms at 48 kHz.
constexpr uint64_t kOpusFrameDurationNs = 20'000'000ULL;
constexpr uint64_t kMatroskaTimecodeScaleNs = 100'000ULL;
constexpr uint64_t kOpusSeekPreRollNs = 80'000'000ULL;

bool set_error(std::string& error, const char* message) {
    error = message;
    return false;
}

bool set_error(std::string& error, const std::string& message) {
    error = message;
    return false;
}

class TemporaryOutput {
public:
    explicit TemporaryOutput(const std::filesystem::path& destination) : destination_(destination) {
        static std::atomic<uint64_t> counter{0};
        const auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
        temporary_ = destination_;
        temporary_ += ".tmp." + std::to_string(ticks) + "." +
                      std::to_string(counter.fetch_add(1, std::memory_order_relaxed));
    }

    ~TemporaryOutput() {
        if (!committed_) {
            std::error_code ignored;
            std::filesystem::remove(temporary_, ignored);
        }
    }

    const std::filesystem::path& path() const { return temporary_; }

    bool commit(std::string& error) {
#if defined(_WIN32)
        if (!MoveFileExW(temporary_.c_str(), destination_.c_str(),
                         MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
            return set_error(error, "failed to atomically replace output file");
        }
#else
        if (std::rename(temporary_.c_str(), destination_.c_str()) != 0) {
            return set_error(error, std::string("failed to atomically replace output file: ") +
                                         std::strerror(errno));
        }
#endif
        committed_ = true;
        return true;
    }

private:
    std::filesystem::path destination_;
    std::filesystem::path temporary_;
    bool committed_ = false;
};

struct PngFileSink {
    std::FILE* file = nullptr;
    bool failed = false;
};

void png_write_callback(void* context, void* data, int size) {
    auto* sink = static_cast<PngFileSink*>(context);
    if (sink->failed || size < 0 ||
        std::fwrite(data, 1, static_cast<size_t>(size), sink->file) != static_cast<size_t>(size)) {
        sink->failed = true;
    }
}

bool valid_png_image(const sd_image_t& image, std::string& error) {
    if (image.data == nullptr || image.width == 0 || image.height == 0) {
        return set_error(error, "PNG input must have nonempty dimensions and pixel data");
    }
    if (image.channel < 1 || image.channel > 4) {
        return set_error(error, "PNG input must have between one and four channels");
    }
    if (image.width > static_cast<uint32_t>(std::numeric_limits<int>::max()) ||
        image.height > static_cast<uint32_t>(std::numeric_limits<int>::max())) {
        return set_error(error, "PNG dimensions exceed the encoder limits");
    }
    const uint64_t stride = static_cast<uint64_t>(image.width) * image.channel;
    if (stride > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
        return set_error(error, "PNG row stride exceeds the encoder limits");
    }
    const uint64_t source_bytes = stride * image.height;
    const uint64_t filtered_bytes = (stride + 1) * image.height;
    if (source_bytes > static_cast<uint64_t>(std::numeric_limits<int>::max()) ||
        source_bytes > static_cast<uint64_t>(std::numeric_limits<size_t>::max()) ||
        filtered_bytes > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
        return set_error(error, "PNG image buffer exceeds the encoder limits");
    }
    return true;
}

bool valid_video_frame(const sd_image_t& frame, uint32_t expected_width,
                       uint32_t expected_height, std::string& error) {
    if (frame.data == nullptr || frame.width == 0 || frame.height == 0) {
        return set_error(error, "video frames must have nonempty dimensions and pixel data");
    }
    if (frame.width != expected_width || frame.height != expected_height) {
        return set_error(error, "all video frames must have identical dimensions");
    }
    if (frame.channel != 3 && frame.channel != 4) {
        return set_error(error, "VP8 video frames must be RGB or RGBA");
    }
    if (frame.width > WEBP_MAX_DIMENSION || frame.height > WEBP_MAX_DIMENSION ||
        frame.width > static_cast<uint32_t>(std::numeric_limits<int>::max()) ||
        frame.height > static_cast<uint32_t>(std::numeric_limits<int>::max())) {
        return set_error(error, "video frame dimensions exceed VP8 limits");
    }
    const uint64_t stride = static_cast<uint64_t>(frame.width) * frame.channel;
    if (stride > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
        return set_error(error, "video frame row stride exceeds VP8 limits");
    }
    return true;
}

bool rounded_sample_count(uint64_t frames, uint64_t rate, uint64_t denominator,
                          uint64_t& result) {
    const long double value = static_cast<long double>(frames) * rate / denominator;
    if (!std::isfinite(value) || value < 1.0L ||
        value > static_cast<long double>(std::numeric_limits<uint64_t>::max()) - 0.5L) {
        return false;
    }
    result = static_cast<uint64_t>(std::llround(value));
    return result > 0;
}

bool rounded_video_timestamp_ns(uint64_t frame_index, int fps, uint64_t& result) {
    const long double value = static_cast<long double>(frame_index) * 1'000'000'000.0L / fps;
    if (!std::isfinite(value) ||
        value > static_cast<long double>(std::numeric_limits<uint64_t>::max()) - 0.5L) {
        return false;
    }
    result = static_cast<uint64_t>(std::llround(value));
    return true;
}

bool resample_interleaved_audio(const sd_audio_t& audio, uint64_t output_samples,
                               std::vector<float>& output,
                               std::string& error) {
    if (audio.data == nullptr || audio.sample_count == 0 || audio.sample_rate < 8000 ||
        audio.sample_rate > 192000 || audio.channels != 2) {
        return set_error(error, "WebM audio must contain stereo samples at 8-192 kHz");
    }
    if (audio.sample_count > static_cast<uint64_t>(std::numeric_limits<long>::max()) ||
        output_samples > static_cast<uint64_t>(std::numeric_limits<long>::max()) ||
        audio.sample_count > std::numeric_limits<size_t>::max() / kOpusChannels ||
        output_samples > std::numeric_limits<size_t>::max() /
                             (sizeof(float) * kOpusChannels)) {
        return set_error(error, "WebM audio sample count exceeds resampler limits");
    }

    const size_t source_samples = static_cast<size_t>(audio.sample_count) * kOpusChannels;
    for (size_t i = 0; i < source_samples; ++i) {
        if (!std::isfinite(audio.data[i])) {
            return set_error(error, "WebM audio contains a non-finite sample");
        }
    }

    output.assign(static_cast<size_t>(output_samples) * kOpusChannels, 0.0f);
    int src_error = 0;
    SRC_STATE* raw_state = src_new(SRC_SINC_BEST_QUALITY, kOpusChannels, &src_error);
    if (raw_state == nullptr) {
        return set_error(error, std::string("could not create audio resampler: ") +
                                     src_strerror(src_error));
    }
    std::unique_ptr<SRC_STATE, decltype(&src_delete)> state(raw_state, &src_delete);

    SRC_DATA request{};
    request.data_in = audio.data;
    request.input_frames = static_cast<long>(audio.sample_count);
    request.data_out = output.data();
    request.output_frames = static_cast<long>(output_samples);
    request.src_ratio = static_cast<double>(kOpusSampleRate) / audio.sample_rate;
    request.end_of_input = 1;
    src_error = src_process(state.get(), &request);
    if (src_error != 0) {
        return set_error(error, std::string("audio resampling failed: ") + src_strerror(src_error));
    }
    if (request.input_frames_used < 0 || request.output_frames_gen < 0 ||
        request.output_frames_gen > request.output_frames) {
        return set_error(error, "audio resampler returned invalid frame counts");
    }
    if (request.input_frames_used < request.input_frames &&
        request.output_frames_gen < request.output_frames) {
        return set_error(error, "audio resampler could not consume the source samples");
    }
    const size_t generated_samples = static_cast<size_t>(request.output_frames_gen) *
                                     kOpusChannels;
    for (size_t i = 0; i < generated_samples; ++i) {
        if (!std::isfinite(output[i])) {
            return set_error(error, "audio resampler produced a non-finite sample");
        }
        output[i] = std::clamp(output[i], -1.0f, 1.0f);
    }
    return true;
}

class CheckedMkvWriter final : public mkvmuxer::IMkvWriter {
public:
    ~CheckedMkvWriter() override { close(); }

    bool open(const std::filesystem::path& path) {
#if defined(_WIN32)
        file_ = _wfopen(path.c_str(), L"wb+");
#else
        file_ = std::fopen(path.c_str(), "wb+");
#endif
        return file_ != nullptr;
    }

    bool close() {
        if (file_ == nullptr) {
            return !failed_;
        }
        if (std::fflush(file_) != 0) {
            failed_ = true;
        }
        if (std::fclose(file_) != 0) {
            failed_ = true;
        }
        file_ = nullptr;
        return !failed_;
    }

    mkvmuxer::int32 Write(const void* buffer, mkvmuxer::uint32 length) override {
        if (file_ == nullptr || failed_ ||
            (length != 0 && std::fwrite(buffer, 1, length, file_) != length)) {
            failed_ = true;
            return -1;
        }
        return 0;
    }

    mkvmuxer::int64 Position() const override {
        if (file_ == nullptr || failed_) {
            return -1;
        }
#if defined(_WIN32)
        return _ftelli64(file_);
#else
        return static_cast<mkvmuxer::int64>(ftello(file_));
#endif
    }

    mkvmuxer::int32 Position(mkvmuxer::int64 position) override {
        if (file_ == nullptr || failed_) {
            return -1;
        }
#if defined(_WIN32)
        if (_fseeki64(file_, position, SEEK_SET) != 0) {
#else
        if (fseeko(file_, static_cast<off_t>(position), SEEK_SET) != 0) {
#endif
            failed_ = true;
            return -1;
        }
        return 0;
    }

    bool Seekable() const override { return true; }

    void ElementStartNotify(mkvmuxer::uint64, mkvmuxer::int64) override {}

private:
    std::FILE* file_ = nullptr;
    bool failed_ = false;
};

bool configure_opus(OpusEncoder* encoder, uint32_t& lookahead, std::string& error) {
    if (opus_encoder_ctl(encoder, OPUS_SET_BITRATE(128000)) != OPUS_OK ||
        opus_encoder_ctl(encoder, OPUS_SET_COMPLEXITY(8)) != OPUS_OK ||
        opus_encoder_ctl(encoder, OPUS_SET_SIGNAL(OPUS_SIGNAL_MUSIC)) != OPUS_OK ||
        opus_encoder_ctl(encoder, OPUS_SET_VBR(1)) != OPUS_OK ||
        opus_encoder_ctl(encoder, OPUS_SET_DTX(0)) != OPUS_OK ||
        opus_encoder_ctl(encoder, OPUS_SET_INBAND_FEC(0)) != OPUS_OK) {
        return set_error(error, "could not configure the Opus encoder");
    }
    int delay = 0;
    if (opus_encoder_ctl(encoder, OPUS_GET_LOOKAHEAD(&delay)) != OPUS_OK || delay < 0 ||
        delay > std::numeric_limits<uint16_t>::max()) {
        return set_error(error, "could not determine the Opus encoder pre-skip");
    }
    lookahead = static_cast<uint32_t>(delay);
    return true;
}

bool encode_and_add_vp8(mkvmuxer::Segment& segment, uint64_t track_number,
                        const sd_image_t& frame, uint64_t timestamp_ns,
                        std::string& error) {
    WebPConfig config;
    if (!WebPConfigPreset(&config, WEBP_PRESET_DEFAULT, 82.0f)) {
        return set_error(error, "could not initialize the VP8 frame encoder");
    }
    config.lossless = 0;
    config.method = 4;
    config.thread_level = 0;
    config.pass = 1;
    if (!WebPValidateConfig(&config)) {
        return set_error(error, "invalid VP8 frame encoder configuration");
    }

    WebPPicture picture;
    if (!WebPPictureInit(&picture)) {
        return set_error(error, "could not initialize the VP8 frame picture");
    }
    picture.width = static_cast<int>(frame.width);
    picture.height = static_cast<int>(frame.height);
    picture.use_argb = 1;

    WebPMemoryWriter encoded;
    WebPMemoryWriterInit(&encoded);
    picture.writer = WebPMemoryWrite;
    picture.custom_ptr = &encoded;
    const int imported = frame.channel == 3
                             ? WebPPictureImportRGB(&picture, frame.data,
                                                   static_cast<int>(frame.width * frame.channel))
                             : WebPPictureImportRGBX(&picture, frame.data,
                                                     static_cast<int>(frame.width * frame.channel));
    if (!imported || !WebPEncode(&config, &picture)) {
        const int encoder_error = static_cast<int>(picture.error_code);
        WebPPictureFree(&picture);
        WebPMemoryWriterClear(&encoded);
        return set_error(error, "VP8 frame encoding failed (WebP error " +
                                     std::to_string(encoder_error) + ")");
    }
    WebPPictureFree(&picture);

    const uint8_t* vp8_data = nullptr;
    size_t vp8_size = 0;
    const uint8_t* bytes = encoded.mem;
    const size_t size = encoded.size;
    bool valid_webp = bytes != nullptr && size >= 12 &&
                      std::memcmp(bytes, "RIFF", 4) == 0 &&
                      std::memcmp(bytes + 8, "WEBP", 4) == 0;
    if (valid_webp) {
        const uint64_t riff_end = 8ULL + static_cast<uint64_t>(bytes[4]) +
                                  (static_cast<uint64_t>(bytes[5]) << 8) +
                                  (static_cast<uint64_t>(bytes[6]) << 16) +
                                  (static_cast<uint64_t>(bytes[7]) << 24);
        valid_webp = riff_end <= size && riff_end >= 12;
        size_t offset = 12;
        while (valid_webp && offset + 8 <= riff_end) {
            const uint32_t chunk_size = static_cast<uint32_t>(bytes[offset + 4]) |
                                        (static_cast<uint32_t>(bytes[offset + 5]) << 8) |
                                        (static_cast<uint32_t>(bytes[offset + 6]) << 16) |
                                        (static_cast<uint32_t>(bytes[offset + 7]) << 24);
            const size_t payload = offset + 8;
            if (chunk_size > riff_end - payload) {
                valid_webp = false;
                break;
            }
            if (std::memcmp(bytes + offset, "VP8 ", 4) == 0) {
                vp8_data = bytes + payload;
                vp8_size = chunk_size;
                break;
            }
            const uint64_t next = static_cast<uint64_t>(payload) + chunk_size + (chunk_size & 1U);
            if (next > riff_end) {
                valid_webp = false;
                break;
            }
            offset = static_cast<size_t>(next);
        }
    }
    if (!valid_webp || vp8_data == nullptr || vp8_size < 10 ||
        (vp8_data[0] & 1U) != 0 || vp8_data[3] != 0x9d ||
        vp8_data[4] != 0x01 || vp8_data[5] != 0x2a) {
        WebPMemoryWriterClear(&encoded);
        return set_error(error, "WebP encoder did not return a valid VP8 keyframe");
    }
    const uint32_t encoded_width = static_cast<uint32_t>(vp8_data[6]) |
                                   ((static_cast<uint32_t>(vp8_data[7]) & 0x3fU) << 8);
    const uint32_t encoded_height = static_cast<uint32_t>(vp8_data[8]) |
                                    ((static_cast<uint32_t>(vp8_data[9]) & 0x3fU) << 8);
    if (encoded_width != frame.width || encoded_height != frame.height) {
        WebPMemoryWriterClear(&encoded);
        return set_error(error, "VP8 encoder changed the input frame dimensions");
    }

    const bool added = segment.AddFrame(vp8_data, vp8_size, track_number, timestamp_ns, true);
    WebPMemoryWriterClear(&encoded);
    if (!added) {
        return set_error(error, "WebM muxer rejected a VP8 frame");
    }
    return true;
}

bool encode_and_add_opus(mkvmuxer::Segment& segment, OpusEncoder* encoder,
                         uint64_t track_number, const std::vector<float>& audio,
                         uint64_t output_samples, uint64_t packet_index,
                         uint64_t packet_count, uint32_t lookahead,
                         std::string& error) {
    std::array<float, kOpusFrameSamples * kOpusChannels> input{};
    const uint64_t sample_offset = packet_index * kOpusFrameSamples;
    for (int i = 0; i < kOpusFrameSamples; ++i) {
        const uint64_t sample = sample_offset + static_cast<uint64_t>(i);
        const float left = sample < output_samples
                               ? audio[static_cast<size_t>(sample) * kOpusChannels]
                               : 0.0f;
        const float right = sample < output_samples
                                ? audio[static_cast<size_t>(sample) * kOpusChannels + 1]
                                : 0.0f;
        input[static_cast<size_t>(i) * kOpusChannels] = left;
        input[static_cast<size_t>(i) * kOpusChannels + 1] = right;
    }

    std::array<unsigned char, 4000> packet{};
    const int encoded_size = opus_encode_float(encoder, input.data(), kOpusFrameSamples,
                                               packet.data(), static_cast<opus_int32>(packet.size()));
    if (encoded_size <= 0) {
        return set_error(error, std::string("Opus encoding failed: ") + opus_strerror(encoded_size));
    }

    const uint64_t timestamp_ns = packet_index * kOpusFrameDurationNs;
    bool added = false;
    if (packet_index + 1 == packet_count) {
        const uint64_t total_packet_samples = packet_count * kOpusFrameSamples;
        const uint64_t tail_padding_samples = total_packet_samples - output_samples - lookahead;
        if (tail_padding_samples > 0) {
            const int64_t discard_padding_ns = static_cast<int64_t>(
                (tail_padding_samples * 1'000'000'000ULL + kOpusSampleRate / 2) /
                kOpusSampleRate);
            added = segment.AddFrameWithDiscardPadding(packet.data(),
                                                       static_cast<uint64_t>(encoded_size),
                                                       discard_padding_ns, track_number,
                                                       timestamp_ns, true);
        } else {
            added = segment.AddFrame(packet.data(), static_cast<uint64_t>(encoded_size),
                                     track_number, timestamp_ns, true);
        }
    } else {
        added = segment.AddFrame(packet.data(), static_cast<uint64_t>(encoded_size),
                                 track_number, timestamp_ns, true);
    }
    if (!added) {
        return set_error(error, "WebM muxer rejected an Opus packet");
    }
    return true;
}

}  // namespace

bool write_png(const std::filesystem::path& path, const sd_image_t& image, std::string& error) {
    error.clear();
    try {
        if (path.empty()) {
            return set_error(error, "PNG output path is empty");
        }
        if (!valid_png_image(image, error)) {
            return false;
        }

        TemporaryOutput output(path);
        PngFileSink sink;
#if defined(_WIN32)
        sink.file = _wfopen(output.path().c_str(), L"wb");
#else
        sink.file = std::fopen(output.path().c_str(), "wb");
#endif
        if (sink.file == nullptr) {
            return set_error(error, std::string("could not open PNG output: ") + std::strerror(errno));
        }

        const int stride = static_cast<int>(static_cast<uint64_t>(image.width) * image.channel);
        const int encoded = stbi_write_png_to_func(png_write_callback, &sink,
                                                   static_cast<int>(image.width),
                                                   static_cast<int>(image.height),
                                                   static_cast<int>(image.channel),
                                                   image.data, stride);
        bool io_failed = sink.failed;
        if (std::fflush(sink.file) != 0) {
            io_failed = true;
        }
        if (std::fclose(sink.file) != 0) {
            io_failed = true;
        }
        if (!encoded) {
            return set_error(error, "PNG encoding failed");
        }
        if (io_failed) {
            return set_error(error, "writing PNG output failed");
        }
        return output.commit(error);
    } catch (const std::exception& exception) {
        return set_error(error, std::string("PNG encoding failed: ") + exception.what());
    } catch (...) {
        return set_error(error, "PNG encoding failed with an unknown error");
    }
}

bool write_webm(const std::filesystem::path& path, const sd_image_t* frames, int frame_count,
                int fps, const sd_audio_t& audio, std::string& error) {
    error.clear();
    try {
        if (path.empty()) {
            return set_error(error, "WebM output path is empty");
        }
        if (frames == nullptr || frame_count <= 0 || fps <= 0 || fps > 240) {
            return set_error(error, "WebM requires frames, a positive frame count, and an fps from 1 to 240");
        }
        if (frames[0].width == 0 || frames[0].height == 0) {
            return set_error(error, "WebM frames must have nonempty dimensions");
        }
        for (int i = 0; i < frame_count; ++i) {
            if (!valid_video_frame(frames[i], frames[0].width, frames[0].height, error)) {
                error = "invalid WebM frame " + std::to_string(i) + ": " + error;
                return false;
            }
        }
        if (audio.data == nullptr || audio.sample_count == 0 || audio.sample_rate == 0 ||
            audio.channels != 2) {
            return set_error(error, "WebM video requires nonempty stereo audio");
        }

        uint64_t video_duration_ns = 0;
        uint64_t output_samples = 0;
        uint64_t natural_samples = 0;
        if (!rounded_sample_count(static_cast<uint64_t>(frame_count), 1'000'000'000ULL,
                                  static_cast<uint64_t>(fps), video_duration_ns) ||
            !rounded_sample_count(static_cast<uint64_t>(frame_count), kOpusSampleRate,
                                  static_cast<uint64_t>(fps), output_samples) ||
            !rounded_sample_count(audio.sample_count, kOpusSampleRate,
                                  audio.sample_rate, natural_samples)) {
            return set_error(error, "WebM duration exceeds supported timestamp limits");
        }
        const uint64_t audio_frame_tolerance =
            (static_cast<uint64_t>(kOpusSampleRate) + static_cast<uint64_t>(fps) - 1) /
            static_cast<uint64_t>(fps);
        const uint64_t duration_difference = natural_samples > output_samples
                                                 ? natural_samples - output_samples
                                                 : output_samples - natural_samples;
        if (duration_difference > audio_frame_tolerance) {
            return set_error(error, "WebM audio duration differs from video by more than one video frame");
        }

        std::vector<float> resampled_audio;
        if (!resample_interleaved_audio(audio, output_samples, resampled_audio, error)) {
            return false;
        }

        int opus_error = OPUS_OK;
        std::unique_ptr<OpusEncoder, decltype(&opus_encoder_destroy)> encoder(
            opus_encoder_create(kOpusSampleRate, kOpusChannels, OPUS_APPLICATION_AUDIO,
                                &opus_error),
            &opus_encoder_destroy);
        if (!encoder || opus_error != OPUS_OK) {
            return set_error(error, std::string("could not create Opus encoder: ") +
                                         opus_strerror(opus_error));
        }
        uint32_t lookahead = 0;
        if (!configure_opus(encoder.get(), lookahead, error)) {
            return false;
        }
        if (output_samples > std::numeric_limits<uint64_t>::max() - lookahead) {
            return set_error(error, "WebM audio duration exceeds Opus packet limits");
        }
        const uint64_t samples_with_lookahead = output_samples + lookahead;
        const uint64_t packet_count =
            (samples_with_lookahead + kOpusFrameSamples - 1) / kOpusFrameSamples;
        if (packet_count == 0 ||
            packet_count > std::numeric_limits<uint64_t>::max() / kOpusFrameSamples) {
            return set_error(error, "WebM audio packet count exceeds supported limits");
        }

        TemporaryOutput output(path);
        CheckedMkvWriter writer;
        if (!writer.open(output.path())) {
            return set_error(error, std::string("could not open WebM output: ") +
                                         std::strerror(errno));
        }
        mkvmuxer::Segment segment;
        segment.set_mode(mkvmuxer::Segment::kFile);
        if (!segment.Init(&writer)) {
            writer.close();
            return set_error(error, "could not initialize WebM muxer");
        }
        segment.GetSegmentInfo()->set_timecode_scale(kMatroskaTimecodeScaleNs);
        segment.set_duration(static_cast<double>(video_duration_ns) /
                             kMatroskaTimecodeScaleNs);
        segment.GetSegmentInfo()->set_muxing_app("gpq/libwebm");
        segment.GetSegmentInfo()->set_writing_app("gpq/libwebm");
        segment.set_max_cluster_duration(5'000'000'000ULL);

        const uint64_t video_track_number =
            segment.AddVideoTrack(static_cast<int32_t>(frames[0].width),
                                  static_cast<int32_t>(frames[0].height), 1);
        const uint64_t audio_track_number =
            segment.AddAudioTrack(kOpusSampleRate, kOpusChannels, 2);
        if (video_track_number == 0 || audio_track_number == 0) {
            writer.close();
            return set_error(error, "could not create WebM video and audio tracks");
        }

        auto* video_track = static_cast<mkvmuxer::VideoTrack*>(
            segment.GetTrackByNumber(video_track_number));
        auto* audio_track = static_cast<mkvmuxer::AudioTrack*>(
            segment.GetTrackByNumber(audio_track_number));
        const uint64_t codec_delay_ns =
            (static_cast<uint64_t>(lookahead) * 1'000'000'000ULL + kOpusSampleRate / 2) /
            kOpusSampleRate;
        std::array<uint8_t, 19> opus_head{{
            'O', 'p', 'u', 's', 'H', 'e', 'a', 'd',
            1, 2,
            static_cast<uint8_t>(lookahead & 0xffU),
            static_cast<uint8_t>((lookahead >> 8) & 0xffU),
            static_cast<uint8_t>(kOpusSampleRate & 0xffU),
            static_cast<uint8_t>((kOpusSampleRate >> 8) & 0xffU),
            static_cast<uint8_t>((kOpusSampleRate >> 16) & 0xffU),
            static_cast<uint8_t>((kOpusSampleRate >> 24) & 0xffU),
            0, 0, 0
        }};
        if (video_track == nullptr || audio_track == nullptr) {
            writer.close();
            return set_error(error, "WebM muxer did not expose the created tracks");
        }
        video_track->set_codec_id("V_VP8");
        video_track->set_default_duration(
            (1'000'000'000ULL + static_cast<uint64_t>(fps) / 2) /
            static_cast<uint64_t>(fps));
        video_track->set_frame_rate(static_cast<double>(fps));
        audio_track->set_codec_id("A_OPUS");
        audio_track->set_default_duration(kOpusFrameDurationNs);
        audio_track->set_codec_delay(codec_delay_ns);
        audio_track->set_seek_pre_roll(kOpusSeekPreRollNs);
        if (!audio_track->SetCodecPrivate(opus_head.data(), opus_head.size())) {
            writer.close();
            return set_error(error, "could not attach OpusHead metadata to WebM audio track");
        }

        uint64_t video_index = 0;
        uint64_t audio_index = 0;
        while (video_index < static_cast<uint64_t>(frame_count) || audio_index < packet_count) {
            uint64_t video_timestamp_ns = std::numeric_limits<uint64_t>::max();
            if (video_index < static_cast<uint64_t>(frame_count) &&
                !rounded_video_timestamp_ns(video_index, fps, video_timestamp_ns)) {
                writer.close();
                return set_error(error, "WebM video timestamp exceeds supported limits");
            }
            const uint64_t audio_timestamp_ns = audio_index * kOpusFrameDurationNs;
            if (video_index < static_cast<uint64_t>(frame_count) &&
                (audio_index >= packet_count || video_timestamp_ns <= audio_timestamp_ns)) {
                if (!encode_and_add_vp8(segment, video_track_number,
                                        frames[static_cast<size_t>(video_index)],
                                        video_timestamp_ns, error)) {
                    writer.close();
                    return false;
                }
                ++video_index;
            } else {
                if (!encode_and_add_opus(segment, encoder.get(), audio_track_number,
                                         resampled_audio, output_samples, audio_index,
                                         packet_count, lookahead, error)) {
                    writer.close();
                    return false;
                }
                ++audio_index;
            }
        }

        if (!segment.Finalize()) {
            writer.close();
            return set_error(error, "WebM muxer failed to finalize the container");
        }
        if (!writer.close()) {
            return set_error(error, "flushing completed WebM output failed");
        }
        std::error_code size_error;
        const uintmax_t output_size = std::filesystem::file_size(output.path(), size_error);
        if (size_error || output_size == 0) {
            return set_error(error, "completed WebM output is missing or empty");
        }
        return output.commit(error);
    } catch (const std::exception& exception) {
        return set_error(error, std::string("WebM encoding failed: ") + exception.what());
    } catch (...) {
        return set_error(error, "WebM encoding failed with an unknown error");
    }
}

}  // namespace gpq
