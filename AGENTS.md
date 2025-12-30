# Repository Guidelines

## Project Structure & Module Organization
- `src/` contains the Rust crate (entry in `src/main.rs`, shared logic in `src/lib.rs`, CAN/MQTT modules in `src/*.rs`).
- `python_test/` holds ad-hoc integration scripts for CAN and MQTT checks.
- `config.toml` stores runtime configuration; `*.dbc` files describe CAN message schemas.
- `justfile` defines common build/test tasks; `target/` is build output (generated).

## Build, Test, and Development Commands
- `just build`: cross-compile release for `aarch64-unknown-linux-gnu`.
- `just check`: type-check for the same target.
- `just test`: run tests via cross for the target.
- `just build_ubuntu`: build for `x86_64-unknown-linux-gnu`.
- `just build_windows`: build for `x86_64-pc-windows-gnu`.
- `cargo build` / `cargo test`: local build and test without cross (useful for quick iteration).
- `just test_heartbeat`: run MQTT heartbeat check with hardcoded host/port in `python_test/heartbeat_mqtt.py`.

## Coding Style & Naming Conventions
- Rust follows standard `rustfmt` defaults (4-space indentation).
- Naming: `snake_case` for modules/functions, `CamelCase` for types/traits, `SCREAMING_SNAKE_CASE` for constants.
- Keep module APIs small and focused; prefer explicit types over implicit inference when clarity helps.

## Testing Guidelines
- Unit tests live in `src/*` under `#[cfg(test)]` modules (e.g., `src/message_type.rs`).
- Run tests with `cargo test` or `just test` (cross).
- `python_test/` scripts are manual/integration checks; name new ones as `*_test.py` or with a clear action verb.

## Commit & Pull Request Guidelines
- Recent commits use short, lowercase summaries (e.g., “small formatting”); keep messages concise and descriptive.
- PRs should include: purpose, affected modules, and test command(s) run.
- If changes affect CAN or MQTT behavior, note any hardware or broker assumptions and config changes.

## Configuration & Secrets
- Update `config.toml` and the `.dbc` files together when message definitions change.
- Do not commit credentials; use environment variables or local overrides as needed.
