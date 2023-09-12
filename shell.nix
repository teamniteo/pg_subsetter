(builtins.getFlake ("git+file://" + toString ./. + "?shallow=1")).devShells.${builtins.currentSystem}.default
