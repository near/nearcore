{
  inputs.nixify.url = "github:rvolosatovs/nixify";

  outputs =
    { nixify, ... }:
    with nixify.lib;
    rust.mkFlake {
      name = "nearcore";
      src = ./.;

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
            pkgs.openssl
          ];

          nativeBuildInputs = [
            pkgs.pkg-config
            pkgs.rustPlatform.bindgenHook
          ];
        } devShells;
    };
}
