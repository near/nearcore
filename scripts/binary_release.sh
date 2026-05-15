#!/bin/bash
# cspell:words czvf objcopy debuglink dsymutil
set -xeo pipefail

release_type="${1:?Release type is required as the first argument}"
upload_action="${2:-}"

case "${release_type}" in
  release|nightly-release|assertions-release|test-features-release)
    ;;
  *)
    echo "Unsupported release type '${release_type}'"
    exit 1
    ;;
esac

case "${upload_action}" in
  "")
    ;;
  "upload-release")
    if [[ "$release_type" != "release" ]]; then
        echo "'upload-release' action is only allowed for 'release' builds"
        exit 1
    fi
    ;;
  *)
    echo "Unsupported upload action '${upload_action}'"
    exit 1
    ;;
esac


commit=$(git rev-parse HEAD)
branch=$(git branch --show-current)
# in case of Release triggered run, branch is empty
if [ -z "${branch}" ]; then
  ref=$(git describe --tags | head -n1)
  branch=$(git branch -r --contains="${ref}" | head -n1 | cut -c3- | cut -d / -f 2)
fi

os=$(uname)
arch=$(uname -m)
os_and_arch=${os}-${arch}

run_cmd() {
  local command=("$@")
  
  if [[ -n "${DRY_RUN}" ]]; then
      echo "DRY RUN MODE: ${command[*]}"
  else
      "${command[@]}"
  fi
}

# Split debug info out of a release binary into a `.debug` sibling file, then
# strip the original. Adds a gnu-debuglink so gdb auto-finds the debug file
# when both are alongside each other. Compresses the debug sections (zlib)
# since uncompressed DWARF for neard runs to ~1 GB; gdb reads the compressed
# form transparently. Caller is responsible for ensuring objcopy/strip exist.
function split_debug_info {
  local binary_path="$1"
  if [[ ! -x "${binary_path}" ]]; then
    return 0
  fi
  run_cmd objcopy --only-keep-debug --compress-debug-sections=zlib "${binary_path}" "${binary_path}.debug"
  run_cmd strip --strip-debug --strip-unneeded "${binary_path}"
  run_cmd objcopy --add-gnu-debuglink="${binary_path}.debug" "${binary_path}"
  run_cmd chmod 644 "${binary_path}.debug"
}

function tar_binary {
  local src="$1"

  run_cmd mkdir -p "${src}/${os_and_arch}"
  run_cmd cp "target/release/${src}" "${src}/${os_and_arch}/"
  run_cmd tar -C "${src}" -czvf "${src}.tar.gz" "${os_and_arch}"
}

function upload_s3 {
  local src="$1"
  local path="$2"

  run_cmd aws s3 cp --acl public-read ${src} s3://build.nearprotocol.com/nearcore/${path}
}

function upload_release_binary {
  local binary="$1"

  tar_binary ${binary}
  local tar_file=${binary}.tar.gz

  # Do not publish the branch name for release events.
  local upload_target=""
  if [ "${GITHUB_EVENT_NAME}" = "release" ] && [ -n "${GITHUB_REF_NAME}" ]; then
    upload_target="${GITHUB_REF_NAME}"
  elif [ -n "${branch}" ]; then
    upload_target="${branch}"
  fi

  if [ -z "${upload_target}" ]; then
    echo "Unable to determine upload target for release artifacts" >&2
    exit 1
  fi

  local sources=("target/release/${binary}" "${tar_file}")
  local destinations=("${binary}" "${tar_file}")
  # Include the split-out debug file if it exists (for post-mortem debugging).
  if [[ -f "target/release/${binary}.debug" ]]; then
    sources+=("target/release/${binary}.debug")
    destinations+=("${binary}.debug")
  fi

  for i in "${!sources[@]}"; do
    local src=${sources[$i]}
    local dst=${destinations[$i]}
    upload_s3 "${src}" "${os_and_arch}/${upload_target}/${dst}"
    upload_s3 "${src}" "${os_and_arch}/${upload_target}/${commit}/${dst}"
    upload_s3 "${src}" "${os_and_arch}/${upload_target}/${commit}/stable/${dst}"
  done
}

function upload_binary {
  local binary="$1"
  local folder="${release_type%-release}"

  local sources=("target/release/${binary}")
  local destinations=("${binary}")
  if [[ -f "target/release/${binary}.debug" ]]; then
    sources+=("target/release/${binary}.debug")
    destinations+=("${binary}.debug")
  fi
  for i in "${!sources[@]}"; do
    upload_s3 "${sources[$i]}" "${os_and_arch}/${branch}/${folder}/${destinations[$i]}"
    upload_s3 "${sources[$i]}" "${os_and_arch}/${branch}/${commit}/${folder}/${destinations[$i]}"
  done
}

# Build with full DWARF, then split debug info into a sibling .debug file
# and strip the binary. The shipped binary stays roughly release-sized; the
# .debug sibling uploads alongside for post-mortem symbol resolution.
#
# Tagged releases (upload_action=upload-release) always include debug info;
# other invocations opt in via WITH_DEBUG_INFO=1. Either way requires
# objcopy/strip, which Linux runners have but macOS doesn't. Mac releases
# use dsymutil/.dSYM separately and stay on the default release profile.
SPLIT_DEBUG_INFO=
if [[ "${upload_action}" == "upload-release" || -n "${WITH_DEBUG_INFO:-}" ]]; then
  if command -v objcopy >/dev/null 2>&1 && command -v strip >/dev/null 2>&1; then
    SPLIT_DEBUG_INFO=1
    export CARGO_PROFILE_RELEASE_DEBUG=2
    export CARGO_PROFILE_RELEASE_STRIP=none
  else
    echo "objcopy/strip unavailable; building without split debug info" >&2
  fi
fi

run_cmd make $release_type

if [[ -n "${SPLIT_DEBUG_INFO}" ]]; then
  split_debug_info "target/release/neard"
  split_debug_info "target/release/near-sandbox"
fi

if [ "$upload_action" = "upload-release" ]
then
  upload_release_binary neard
  # near-sandbox is used by near-workspaces which is an SDK for end-to-end contracts testing that automatically 
  # spins up localnet using near-sandbox (neard with extra features useful for testing - state patching, time travel). 
  # There are JS and Rust SDKs and it wouldn’t be efficient to build nearcore from scratch on the 
  # user machine and CI, so it relies on the prebuilt binaries.
  # example PR https://github.com/near/near-sandbox/pull/81/files
  upload_release_binary near-sandbox
else
  upload_binary neard
fi