# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DataFusionSharp provides .NET bindings for Apache DataFusion, a high-performance query engine built on Apache Arrow. The library uses a hybrid architecture with C# for the public API and Rust for the native interop layer.

**Current Status:** Early-stage (v0.1.0), Linux x64 only

## Build Commands

```bash
# Build entire solution (automatically compiles Rust native library)
dotnet build

# Build release
dotnet build -c Release

# Run tests
dotnet test

# Run example
dotnet run --project examples/QueryCsv/QueryCsv.csproj

# Native-only build (from native/ directory)
cargo build --profile dev --target x86_64-unknown-linux-gnu
```

**Prerequisites:** .NET 10.0 SDK, Rust 1.93+ toolchain

## Architecture

Three layers:

### Layer 1: Public C# API (`src/DataFusionSharp/`)
- `DataFusionRuntime` - Tokio runtime wrapper, manages async executor lifecycle
- `SessionContext` - SQL execution context, registers tables and executes queries
- `DataFrame` - Query result wrapper with async operations (Count, Show, Schema, Collect)
- `DataFusionException` - Error handling with error codes from native layer

### Layer 2: Interop Bridge (`src/DataFusionSharp/Interop/`)
- `NativeMethods.cs` - P/Invoke declarations using `LibraryImport`
- `AsyncCallback.cs` / `AsyncOperations.cs` - Callback-based async pattern bridging Rust futures to C# Tasks
- `BytesData.cs` / `ErrorInfoData.cs` - Marshaling structures for cross-language data

### Layer 3: Native Rust Library (`native/src/`)
- `runtime.rs` - `datafusion_runtime_*` exports for Tokio runtime
- `context.rs` - `datafusion_context_*` exports for SessionContext
- `dataframe.rs` - `datafusion_dataframe_*` exports for DataFrame operations
- `callback.rs` - Callback invocation to C#
- `error.rs` - Error codes matching `DataFusionErrorCode` enum

### Async Pattern
C# calls native function with callback pointer and userData → Rust spawns Tokio task → invokes callback on completion → C# `TaskCompletionSource` bridges to `async/await`.

### Memory Management
**Handles (long-lived):** Rust owns objects; C# holds opaque `IntPtr`, calls destroy via `IDisposable`. Double-free protection: set `_handle = IntPtr.Zero` before destroy. Children hold strong refs to parents (DataFrame → SessionContext → Runtime).

**Transient data (strings, buffers, callbacks):** Caller owns memory; callee copies if it needs to retain.

## Adding New DataFrame Operations

When adding a new DataFrame operation (e.g., `Limit`, `Select`):

1. **Rust side** (`native/src/dataframe.rs`): Add `#[no_mangle] pub extern "C" fn datafusion_dataframe_<operation>(...)`
2. **C# Interop** (`src/DataFusionSharp/Interop/NativeMethods.cs`): Add `[LibraryImport]` declaration
3. **C# API** (`src/DataFusionSharp/DataFrame.cs`): Add public async method calling through interop
4. **Tests** (`tests/DataFusionSharp.Tests/`): Add test coverage

## Commit Convention

Use [Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `perf:`, `build:`, `ci:`, `chore:`

Examples: `feat: add DataFrame.Limit()`, `fix: memory leak in native layer`
