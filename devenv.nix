{ pkgs, lib, config, inputs, ... }:

{
  # https://devenv.sh/basics/
  env.GREET = "devenv";

  # https://devenv.sh/packages/
  packages = [
      pkgs.curl
      pkgs.git
      pkgs.jq
      pkgs.just

      pkgs.rustup
      pkgs.sccache
      pkgs.cargo-outdated
      pkgs.cargo-nextest
      pkgs.cargo-audit
      pkgs.rust-analyzer
  ];

  # https://devenv.sh/languages/
  languages.nix.enable = true;

  languages.rust.enable = true;
  languages.rust.channel = "nightly";
  languages.rust.components = [ "rustc" "cargo" "rust-src" "rust-std" ];

  env.RUSTC_WRAPPER = "";
  # languages.rust.mold.enable = true;

  # https://devenv.sh/processes/
  # processes.cargo-watch.exec = "cargo-watch";

  # https://devenv.sh/services/
  # services.postgres.enable = true;

  services.clickhouse.enable = true;
  services.clickhouse.httpPort = 8111;
  services.clickhouse.port = 9111;
  # environment.etc = {
  #   # With changes from https://theorangeone.net/posts/calming-down-clickhouse/
  #   "clickhouse-server/config.d/custom.xml".source = lib.mkForce ./clickhouse-config.xml;
  #   "clickhouse-server/users.d/custom.xml".source = lib.mkForce ./clickhouse-users.xml;
  # };

  env.KLICKHOUSE_TEST_ADDR = "127.0.0.1:9111";

  # # https://devenv.sh/scripts/
  # scripts.hello.exec = ''
  #   echo hello from $GREET
  # '';

  # enterShell = ''
  #   hello
  #   git --version
  # '';

  # https://devenv.sh/tasks/
  # tasks = {
  #   "myproj:setup".exec = "mytool build";
  #   "devenv:enterShell".after = [ "myproj:setup" ];
  # };

  # https://devenv.sh/tests/
  enterTest = ''
    echo "Running tests"
    git --version | grep --color=auto "${pkgs.git.version}"
  '';

  # https://devenv.sh/pre-commit-hooks/
  # pre-commit.hooks.shellcheck.enable = true;

  # See full reference at https://devenv.sh/reference/options/
}
