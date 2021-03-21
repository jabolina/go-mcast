#!/usr/bin/env bash

set -e

echo "Starting stalling external dependencies"

# First dependency is gotestsum for testing.
# More information can be found here:
# https://github.com/gotestyourself/gotestsum
GOTESTSUM=${GOTESTSUM:-"1.6.2"}

if ! command -v gotestsum &> /dev/null
then
    echo "installing gotestsum..."
    curl -sL https://github.com/gotestyourself/gotestsum/releases/download/v${GOTESTSUM}/gotestsum_${GOTESTSUM}_linux_amd64.tar.gz \
      -o ./gotestsum.tar.gz

    mkdir gotestsum && tar xzvf gotestsum.tar.gz -C gotestsum
    alias gotestsum="${PWD}/gotestsum/gotestsum"
    echo "Installed gotestsum $(gotestsum --version)"
fi

echo "Done stalling external dependencies"
