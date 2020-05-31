#!/usr/bin/env bash

# Determine the arch/os combos we're building for
XC_ARCH=${XC_ARCH:-"386 amd64 arm"}
XC_OS=${XC_OS:-linux darwin windows freebsd openbsd solaris}
XC_EXCLUDE_OSARCH="!darwin/arm !darwin/386"

# If its dev mode, only build for ourselves
if [[ -n "${TF_DEV}" ]]; then
    echo "==> Development mode"
    XC_OS=$(go env GOOS)
    XC_ARCH=$(go env GOARCH)

    # Allow LD_FLAGS to be appended during development compilations
    LD_FLAGS="-X main.GitCommit=${GIT_COMMIT}${GIT_DIRTY} $LD_FLAGS"
fi

# Instruct gox to build statically linked binaries
export CGO_ENABLED=0

# In release mode we don't want debug information in the binary
if [[ -n "${TF_RELEASE}" ]]; then
    LD_FLAGS="-s -w"
fi

# Ensure all remote modules are downloaded and cached before build so that
# the concurrent builds launched by gox won't race to redundantly download them.
go mod download

# Build!
echo "==> Building..."
go build -v ./...

# Move all the compiled things to the $GOPATH/bin
GOPATH=${GOPATH:-$(go env GOPATH)}
case $(uname) in
    CYGWIN*)
        GOPATH="$(cygpath $GOPATH)"
        ;;
esac
OLDIFS=$IFS
IFS=: MAIN_GOPATH=($GOPATH)
IFS=$OLDIFS

# Create GOPATH/bin if it's doesn't exists
if [ ! -d $MAIN_GOPATH/bin ]; then
    echo "==> Creating GOPATH/bin directory..."
    mkdir -p $MAIN_GOPATH/bin
fi

# Done!
echo
echo "==> Done"
