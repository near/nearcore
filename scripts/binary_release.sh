#!/bin/bash
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

# cspell:words czvf
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

  local release_tag=""
  # the release commit is assigned multiple tags. Only use the tag from the git run.
  if [ "${GITHUB_EVENT_NAME}" = "release" ] && [ -n "${GITHUB_REF_NAME}" ]; then
    release_tag="${GITHUB_REF_NAME}"
  fi

  # TODO: Do not publish the branch name for release events.
  local upload_targets=()
  if [ -n "${branch}" ]; then
    upload_targets+=("${branch}")
  fi
  if [ -n "${release_tag}" ] && [ "${branch}" != "${release_tag}" ]; then
    upload_targets+=("${release_tag}")
  fi

  if [ ${#upload_targets[@]} -eq 0 ]; then
    echo "Unable to determine upload target for release artifacts" >&2
    exit 1
  fi

  local sources=("target/release/${binary}" "${tar_file}")
  local destinations=("${binary}" "${tar_file}")
  
  for target in "${upload_targets[@]}"; do  
    for i in "${!sources[@]}"; do
      local src=${sources[$i]}
      local dst=${destinations[$i]}
      upload_s3 "${src}" "${os_and_arch}/${target}/${dst}"
      upload_s3 "${src}" "${os_and_arch}/${target}/${commit}/${dst}"
      upload_s3 "${src}" "${os_and_arch}/${target}/${commit}/stable/${dst}"
    done
  done
}

function upload_binary {
  local binary="$1"
  local folder="${release_type%-release}"

  upload_s3 "target/release/${binary}" "${os_and_arch}/${branch}/${commit}/${folder}/${binary}"
}

run_cmd make $release_type

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