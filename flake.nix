{
  inputs.nixify.inputs.nixpkgs-darwin.follows = "nixpkgs";
  inputs.nixify.inputs.nixpkgs-nixos.follows = "nixpkgs";
  inputs.nixify.url = "github:rvolosatovs/nixify";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/release-25.05";

  outputs =
    {
      self,
      nixify,
      ...
    }:
    with nixify.lib;
    rust.mkFlake {
      name = "nearcore";
      src = self;

      buildOverrides =
        {
          pkgs,
          pkgsCross ? pkgs,
          ...
        }:
        {
          buildInputs ? [ ],
          nativeBuildInputs ? [ ],
          ...
        }:
        {
          buildInputs = buildInputs ++ [
            pkgsCross.openssl
          ];

          nativeBuildInputs = nativeBuildInputs ++ [
            pkgs.pkg-config
            pkgs.rustPlatform.bindgenHook
          ];
        };

      withDevShells =
        {
          devShells,
          pkgs,
          ...
        }:
        extendDerivations {
          buildInputs = [
            pkgs.lld
            pkgs.openssl
            pkgs.rocksdb
          ];

          nativeBuildInputs = [
            pkgs.near-cli
            pkgs.pkg-config
            pkgs.rustPlatform.bindgenHook
            pkgs.sccache
          ];

          env.RUSTC_WRAPPER = "${pkgs.sccache}/bin/sccache";
          env.SCCACHE_CACHE_SIZE = "30G";
          env.ROCKSDB_LIB_DIR = "${pkgs.rocksdb}/lib";
          env.RUSTFLAGS = "-C link-arg=-fuse-ld=lld";
        } devShells;
    };
}
