# Run one foreground Worker everywhere

`gpq-worker run` is the common implementation, wrapped by systemd on Linux, launchd and Homebrew services on macOS, and a target-specific native Windows Service adapter; install, uninstall, start, and stop commands manage those wrappers. cargo-dist ships only the Worker binary for supported operating systems, Remote primarily ships as a Linux OCI image, and GPU drivers, Python, llama.cpp, mlx-dspark, and ComfyUI remain operator-installed.
