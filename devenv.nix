{ pkgs, lib, config, inputs, ... }:

{
  # https://devenv.sh/packages/
  packages = [
    pkgs.curl
    pkgs.git
    pkgs.jq
    pkgs.just
    
    pkgs.cargo-outdated
    pkgs.cargo-nextest
    pkgs.cargo-audit
  ];

  # https://devenv.sh/languages/
  languages.nix.enable = true;

  languages.rust.enable = true;
  languages.rust.channel = "nightly";
  languages.rust.components = [ "rustc" "cargo" "clippy" "rustfmt" "rust-src" "rust-std" ];

  # https://devenv.sh/services/
  services.clickhouse.enable = true;
  services.clickhouse.httpPort = 8111;
  services.clickhouse.port = 9111;

  env.KLICKHOUSE_TEST_ADDR = "127.0.0.1:9111";

  # https://devenv.sh/git-hooks/
  git-hooks.hooks = {
    rustfmt.enable = true;
    clippy = {
      enable = true;
      settings.allFeatures = true;
    };
  };
}
