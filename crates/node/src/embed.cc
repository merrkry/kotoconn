// A C ABI over the Node.js C++ embedder API.
//
// The embedder API passes standard library types, so this file must be compiled
// with the C++ standard library that libnode uses. The libnode workflow builds
// both with the same toolchain.
#include <node.h>
#include <node_api.h>

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#define EXPORT __attribute__((visibility("default")))

extern "C" {
typedef void (*kotoconn_node_message)(const char* message, void* data);
typedef void (*kotoconn_node_exit)(int code, void* data);
}

namespace {

std::once_flag initialized;
// Never freed: environments on other threads may still use it while the process exits.
node::MultiIsolatePlatform* platform = nullptr;
std::vector<std::string>* initialization_errors = nullptr;

void Initialize() {
  // The embedder owns signals, stdio and NODE_OPTIONS.
  uint64_t flags = node::ProcessInitializationFlags::kNoInitializeV8 |
                   node::ProcessInitializationFlags::kNoInitializeNodeV8Platform |
                   node::ProcessInitializationFlags::kNoDefaultSignalHandling |
                   node::ProcessInitializationFlags::kNoStdioInitialization |
                   node::ProcessInitializationFlags::kDisableNodeOptionsEnv |
                   node::ProcessInitializationFlags::kNoPrintHelpOrVersionOutput;
  auto result = node::InitializeOncePerProcess(
      {"kotoconn"}, static_cast<node::ProcessInitializationFlags::Flags>(flags));
  initialization_errors = new std::vector<std::string>(result->errors());
  if (result->early_return() != 0) {
    if (initialization_errors->empty()) {
      initialization_errors->push_back("Node.js initialization failed with exit code " +
                                      std::to_string(result->exit_code()));
    }
    return;
  }

  platform = node::MultiIsolatePlatform::Create(4).release();
  v8::V8::InitializePlatform(platform);
  v8::V8::Initialize();
}

}  // namespace

// Stops one environment from any thread, including one that has not started yet.
struct kotoconn_node_stop {
  std::mutex mutex;
  node::Environment* env = nullptr;
  bool requested = false;
};

extern "C" {

// Initializes Node.js and V8 once per process; later calls repeat the first result.
// Reports each initialization error and returns false when initialization failed.
EXPORT bool kotoconn_node_initialize(kotoconn_node_message on_error, void* data) {
  std::call_once(initialized, Initialize);
  for (const auto& error : *initialization_errors) on_error(error.c_str(), data);
  return platform != nullptr;
}

EXPORT kotoconn_node_stop* kotoconn_node_stop_new() { return new kotoconn_node_stop(); }

EXPORT void kotoconn_node_stop_free(kotoconn_node_stop* stop) { delete stop; }

// Stops the environment as soon as it runs, even while JavaScript is executing.
EXPORT void kotoconn_node_stop_request(kotoconn_node_stop* stop) {
  std::lock_guard lock(stop->mutex);
  stop->requested = true;
  if (stop->env == nullptr) return;
  // Stop is not documented as thread-safe; interrupts run on the environment's thread,
  // and FreeEnvironment drains the ones that arrive after its loop ends.
  node::RequestInterrupt(
      stop->env, [](void* env) { node::Stop(static_cast<node::Environment*>(env)); }, stop->env);
}

// Runs one environment on the calling thread until its event loop ends or it is
// stopped. `argv` becomes `process.argv`. `bootstrap` runs as the main script with
// `require` and `process` in scope. `binding` registers the Node-API module that
// `process._linkedBinding(binding_name)` returns. `process.exit()` stops the
// environment and reports its code instead of exiting the process.
// Returns the event loop's exit code, or -1 when the environment could not start.
EXPORT int kotoconn_node_run(const char* const* argv, size_t argc, const char* bootstrap,
                      const char* binding_name, napi_addon_register_func binding,
                      int32_t napi_version, kotoconn_node_stop* stop, kotoconn_node_exit on_exit,
                      kotoconn_node_message on_error, void* data) {
  std::vector<std::string> args(argv, argv + argc);
  std::vector<std::string> exec_args;
  std::vector<std::string> errors;
  // Without an inspector per environment, several environments can share the process.
  auto flags = static_cast<node::EnvironmentFlags::Flags>(
      node::EnvironmentFlags::kNoCreateInspector | node::EnvironmentFlags::kNoGlobalSearchPaths |
      node::EnvironmentFlags::kTrackUnmanagedFds);
  auto setup = node::CommonEnvironmentSetup::Create(platform, &errors, args, exec_args, flags);
  if (!setup) {
    for (const auto& error : errors) on_error(error.c_str(), data);
    return -1;
  }

  v8::Isolate* isolate = setup->isolate();
  node::Environment* env = setup->env();
  int exit_code = -1;
  {
    v8::Locker locker(isolate);
    v8::Isolate::Scope isolate_scope(isolate);
    v8::HandleScope handle_scope(isolate);
    v8::Context::Scope context_scope(setup->context());

    node::AddLinkedBinding(env, binding_name, binding, napi_version);
    node::SetProcessExitHandler(env, [on_exit, data](node::Environment* env, int code) {
      on_exit(code, data);
      node::Stop(env);
    });

    bool requested;
    {
      std::lock_guard lock(stop->mutex);
      stop->env = env;
      requested = stop->requested;
    }

    if (!requested) {
      v8::MaybeLocal<v8::Value> loaded = node::LoadEnvironment(env, bootstrap);
      if (!loaded.IsEmpty()) exit_code = node::SpinEventLoop(env).FromMaybe(-1);
    }

    {
      std::lock_guard lock(stop->mutex);
      stop->env = nullptr;
    }
    node::Stop(env);
  }
  return exit_code;
}

}  // extern "C"
