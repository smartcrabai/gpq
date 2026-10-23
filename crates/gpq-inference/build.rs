use std::env;
use std::fs;
use std::path::Path;

fn main() {
    for path in [
        "native/CMakeLists.txt",
        "native/bridge.cpp",
        "native/media.cmake",
        "native/media.cpp",
        "native/media.hpp",
        "native/apply_stable_diffusion_patch.cmake",
        "native/patches/stable-diffusion-vulkan-lora-cpy.patch",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
    for variable in [
        "CMAKE_GENERATOR",
        "CMAKE_TOOLCHAIN_FILE",
        "CMAKE_PREFIX_PATH",
        "CC",
        "CXX",
        "CFLAGS",
        "CXXFLAGS",
        "VULKAN_SDK",
        "CUDA_PATH",
        "GPQ_VULKAN_IMPORT_LIB_ARM64",
        "MACOSX_DEPLOYMENT_TARGET",
    ] {
        println!("cargo:rerun-if-env-changed={variable}");
    }

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    assert!(
        !(feature("metal") == "ON" && target_os != "macos"),
        "the metal feature requires a macOS target"
    );
    assert!(
        !(feature("cuda") == "ON" && target_os == "macos"),
        "the cuda feature is unavailable on macOS"
    );

    let mut config = cmake::Config::new("native");
    config
        .define("GPQ_METAL", feature("metal"))
        .define("GPQ_VULKAN", feature("vulkan"))
        .define("GPQ_CUDA", feature("cuda"))
        .define("CMAKE_POSITION_INDEPENDENT_CODE", "ON");
    if target_os == "macos"
        && let Ok(deployment_target) = env::var("MACOSX_DEPLOYMENT_TARGET")
    {
        config.define("CMAKE_OSX_DEPLOYMENT_TARGET", deployment_target);
    }
    if feature("vulkan") == "ON" && target_os == "windows" && target_arch == "aarch64" {
        let import_library = env::var("GPQ_VULKAN_IMPORT_LIB_ARM64").unwrap_or_else(|_| {
            panic!("GPQ_VULKAN_IMPORT_LIB_ARM64 is required for Windows ARM64 Vulkan builds")
        });
        config.define("Vulkan_LIBRARY", import_library);
    }
    let output = config.build();

    emit_link_manifest(&output);

    match env::var("CARGO_CFG_TARGET_OS").as_deref() {
        Ok("macos") => println!("cargo:rustc-link-lib=dylib=c++"),
        Ok("linux" | "freebsd" | "dragonfly" | "netbsd" | "openbsd") => {
            println!("cargo:rustc-link-lib=dylib=stdc++");
        }
        _ => {}
    }
}
fn emit_link_manifest(output: &Path) {
    let manifest = output.join("share/gpq/gpq-native-link-items.txt");
    let contents = fs::read_to_string(&manifest).unwrap_or_else(|error| {
        panic!(
            "could not read native link manifest {}: {error}",
            manifest.display()
        )
    });
    let mut next_framework = false;
    let mut entry_count = 0;
    for line in contents.lines() {
        assert!(
            !line.is_empty(),
            "native link manifest contains an empty entry"
        );
        entry_count += 1;
        let (kind, value) = line
            .split_once('|')
            .unwrap_or_else(|| panic!("invalid entry in native link manifest: {line}"));
        assert!(
            !value.is_empty(),
            "native link manifest has an empty value for {kind}"
        );
        assert!(
            !(kind == "SYSTEM" && value.contains("::")),
            "native link manifest contains an unresolved CMake target or directory marker: {value}"
        );
        assert!(
            !(next_framework && kind != "SYSTEM"),
            "native link manifest framework entry is not followed by a system library: {line}"
        );
        match kind {
            "STATIC" => emit_library(Path::new(value), false),
            "DYNAMIC" => emit_library(Path::new(value), true),
            "SYSTEM" if next_framework => {
                println!("cargo:rustc-link-lib=framework={value}");
                next_framework = false;
            }
            "SYSTEM" => emit_system_library(value),
            "FRAMEWORK_NEXT" if value == "framework" || value == "weak_framework" => {
                assert!(
                    !next_framework,
                    "native link manifest contains consecutive framework markers"
                );
                next_framework = true;
            }
            "FRAMEWORK_NEXT" => {
                panic!("unsupported framework linker kind in native link manifest: {value}")
            }
            "OPTION" if value == "-pthread" => {
                println!("cargo:rustc-link-lib=dylib=pthread");
            }
            "OPTION" => panic!("unsupported native link option: {value}"),
            _ => panic!("unknown native link manifest entry kind: {kind}"),
        }
    }
    assert!(
        entry_count != 0,
        "native link manifest contains no link entries"
    );
    assert!(
        !next_framework,
        "native link manifest ended before framework name"
    );
    println!(
        "cargo:rustc-link-search=native={}",
        output.join("lib").display()
    );
}

fn feature(name: &str) -> &'static str {
    if env::var_os(format!("CARGO_FEATURE_{}", name.to_ascii_uppercase())).is_some() {
        "ON"
    } else {
        "OFF"
    }
}

fn emit_library(path: &Path, dynamic: bool) {
    if let Some(framework) = framework_name(path) {
        println!("cargo:rustc-link-lib=framework={framework}");
        return;
    }
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        panic!(
            "invalid library path in native link manifest: {}",
            path.display()
        );
    };
    let stem = library_stem(file_name);
    assert!(
        !stem.is_empty(),
        "could not determine library name from {}",
        path.display()
    );
    if let Some(parent) = path.parent() {
        println!("cargo:rustc-link-search=native={}", parent.display());
    }
    let linkage = if dynamic { "dylib" } else { "static" };
    println!("cargo:rustc-link-lib={linkage}={stem}");
}

fn emit_system_library(value: &str) {
    assert!(
        !value.is_empty(),
        "native link manifest contains an empty system library name"
    );
    let path = Path::new(value);
    if path.is_absolute() {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_else(|| {
                panic!("invalid system library path in native link manifest: {value}")
            });
        let lower_file_name = file_name.to_ascii_lowercase();
        let dynamic = [".dylib", ".so", ".dll", ".tbd"]
            .iter()
            .any(|suffix| lower_file_name.contains(suffix));
        emit_library(path, dynamic);
        return;
    }
    if let Some(name) = value.strip_prefix("-l") {
        assert!(
            !name.is_empty(),
            "native link manifest contains an empty -l library name"
        );
        println!("cargo:rustc-link-lib=dylib={name}");
    } else if let Some(name) = value.strip_prefix("-framework ") {
        assert!(
            !name.is_empty(),
            "native link manifest contains an empty framework name"
        );
        println!("cargo:rustc-link-lib=framework={name}");
    } else {
        let name = value.strip_prefix("lib").unwrap_or(value);
        assert!(
            !name.is_empty(),
            "native link manifest contains an empty system library name"
        );
        println!("cargo:rustc-link-lib=dylib={name}");
    }
}

fn framework_name(path: &Path) -> Option<String> {
    for component in path.components() {
        let text = component.as_os_str().to_str()?;
        if let Some(name) = text.strip_suffix(".framework") {
            return Some(name.to_owned());
        }
    }
    None
}

fn library_stem(file_name: &str) -> String {
    let mut stem = file_name;
    if let Some(rest) = stem.strip_prefix("lib") {
        stem = rest;
    }
    for suffix in [".dylib", ".dll.a", ".tbd", ".dll", ".so", ".a", ".lib"] {
        if let Some(index) = stem.find(suffix) {
            stem = &stem[..index];
            break;
        }
    }
    stem.to_owned()
}
