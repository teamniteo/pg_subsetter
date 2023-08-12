{
  nixConfig = {
    allowed-users = [ "@wheel" "@staff" ]; # allow compiling on every device/machine
  };
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
  };
  outputs = inputs@{ self, nixpkgs, flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {

      systems = nixpkgs.lib.systems.flakeExposed;
      imports = [
        inputs.flake-parts.flakeModules.easyOverlay
      ];

      perSystem = { config, self', inputs', pkgs, system, ... }:
        let

          # dev env without compile tools
          stdenvMinimal = pkgs.stdenvNoCC.override {
            cc = null;
            preHook = "";
            allowedRequisites = null;
            initialPath = pkgs.lib.filter
              (a: pkgs.lib.hasPrefix "coreutils" a.name)
              pkgs.stdenvNoCC.initialPath;
            extraNativeBuildInputs = [ ];
          };
        in
        {
          devShells.default = pkgs.mkShell {
            stdenv = stdenvMinimal;
            packages = with pkgs; [
              go
              goreleaser
              golangci-lint
              postgresql_15
              process-compose
              nixpkgs-fmt
              pgweb
            ];
          };
        };
    };
}
