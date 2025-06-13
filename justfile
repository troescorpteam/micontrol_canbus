build:
  cross build --target aarch64-unknown-linux-gnu --release

check:
  cross check --target aarch64-unknown-linux-gnu

test:
  cross test --target aarch64-unknown-linux-gnu

build_ubuntu:
  cross build --target x86_64-unknown-linux-gnu

build_windows:
  cross build --target x86_64-pc-windows-gnu

remove_deps:
    cargo machete --with-metadata

check_timing:
    cargo build --timings

remove_unused_features:
    cargo features prune