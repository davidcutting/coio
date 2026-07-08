{
  description = "Coio Development";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
  };

  outputs = inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux" "aarch64-linux"
      ];
      perSystem = { config, self', inputs', pkgs, system, ... }: {

        devShells = {
          default = pkgs.mkShell.override { stdenv = pkgs.clangStdenv; } {
            packages = with pkgs; [
              clang-tools
              lldb
              cmake
              ninja
              pkg-config

              lcov
              gcovr
              perf
              gdb

              gtest
              gbenchmark
              liburing
            ];
          };
        };
      };
    };
}
