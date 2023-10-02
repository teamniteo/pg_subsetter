{ pkgs, lib, rootDir, ... }:

{
  # See https://devenv.sh/getting-started/ for more information

  packages = with pkgs;
    [
      entr # Run arbitrary commands when files change
      gitAndTools.gh # GitHub CLI
      heroku # Heroku CLI
      process-compose # Run multiple processes in a single terminal
      golangci-lint # Linter for Go
      postgresql_15 # PostgreSQL database
      eclint # EditorConfig linter and fixer
      gnumake # GNU Make
      goreleaser # Go binary release tool
      pgweb # PostgreSQL web interface
    ];

  languages.javascript.enable = true;
  languages.go.enable = true;
  languages.go.package = pkgs.go_1_21;


  pre-commit.hooks = {
    shellcheck.enable = true;
    nixpkgs-fmt.enable = true;
    gofmt.enable = true;
    shfmt.enable = true;
    golangci-lint = {
      enable = false;
      pass_filenames = false;
      name = "golangci-lint";
      files = ".*";
      entry = "bash -c 'cd $(${rootDir})/backend; ${pkgs.golangci-lint}/bin/golangci-lint run --fix'";
    };
    eclint = {
      enable = true;
      pass_filenames = false;
      name = "eclint";
      files = ".*";
      entry = "${pkgs.eclint}/bin/eclint --fix";
    };
  };


}
