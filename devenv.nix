{
  pkgs,
  lib,
  config,
  inputs,
  ...
}: {
  packages = with pkgs; [
    alejandra # nix formatter
    heroku # Heroku CLI
    postgresql_15 # PostgreSQL database
    gnumake # GNU Make
    goreleaser # Go binary release tool
  ];
  languages.nix.enable = true;
  languages.go.enable = true;

  # https://devenv.sh/tests/
  enterTest = ''
    go test ./...
    go vet ./...
    go mod tidy
    go mod verify
    go build .
  '';
  process.managers.process-compose.tui.enable = false;
  processes = {
    db.exec = ".pgsql/run.sh";
  };

  # https://devenv.sh/pre-commit-hooks/
  pre-commit.hooks = {
    alejandra.enable = true;
    gofmt.enable = true;
  };
  # See full reference at https://devenv.sh/reference/options/
}
