let
  pkgs = import <nixpkgs> { };
in
pkgs.mkShell {
  name = "env";
  hardeningDisable = [ "fortify" ];
  buildInputs = with pkgs; [
    go_1_24
    gotools
    delve
    revive
    duckdb
    protobuf
    protoc-gen-go
  ];
  shellHook = ''
    export PATH="$(go env GOPATH)/bin:$PATH"
    export GOPATH="$(go env GOPATH)"
  '';
}

