# Minimal flake layer to support nix-shell and devenv
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-root.url = "github:srid/flake-root";
    devenv = {
      url = "github:cachix/devenv";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = inputs@{ flake-parts, nixpkgs, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        inputs.devenv.flakeModule
        inputs.flake-root.flakeModule
      ];
      systems = nixpkgs.lib.systems.flakeExposed;
      perSystem = { config, self', inputs', pkgs, system, lib, ... }:
        let
          rootDir = lib.getExe config.flake-root.package;
        in
        {
          devenv.shells.default =
            (import ./devenv.nix {
              inherit inputs pkgs lib rootDir;
            });
        };
    };
}
