#-----------------------------------------------------------------------------
# Global Variables
#-----------------------------------------------------------------------------
GO_VERSION_REQUIRED:=1.10
# Current version, updated after a release.
VERSION ?= 0.1.0
export VERSION
# Cumulatively track the directories/files to delete after a clean
DIRS_TO_CLEAN:=
FILES_TO_CLEAN:=

# Set GOPATH env
SRCPATH ?= $(shell cd ../../../../../../; pwd)
BAUDPATH = $(SRCPATH)/src/github.com/tiglabs/baudengine
GOPATH = $(SRCPATH)
export GOPATH
export BAUD_BUILD=$(BAUDPATH)/build
export BAUD_OUT=$(SRCPATH)/bin
export GOARCH ?= amd64
LOCAL_OS := $(shell uname)
ifeq ($(LOCAL_OS),Linux)
   export GOOS ?= linux
else ifeq ($(LOCAL_OS),Darwin)
   export GOOS ?= darwin
else
   $(error "This system's OS $(LOCAL_OS) isn't recognized/supported")
endif
# It's more concise to use GO?=$(shell which go)
ifeq ($(origin GO), undefined)
  GO:=$(shell which go)
endif
ifeq ($(GO),)
  $(error Could not find ${GO} in path.  Please install go, or if already installed either add it to your path or set GO to point to its directory)
endif

#-----------------------------------------------------------------------------
# Output control
#-----------------------------------------------------------------------------
# Invoke make VERBOSE=1 to enable echoing of the command being executed
export VERBOSE ?= 0
# Place the variable Q in front of a command to control echoing of the command being executed.
Q = $(if $(filter 1,$VERBOSE),,@)
# Use the variable H to add a header (equivalent to =>) to informational output
H = $(shell printf "\033[34;1m=>\033[0m")
# To build with debugger information, use DEBUG=1 when invoking make
ifeq ($(origin DEBUG), undefined)
BUILDTYPE_DIR:=release
else ifeq ($(DEBUG),0)
BUILDTYPE_DIR:=release
else
BUILDTYPE_DIR:=debug
export GCFLAGS:=-N -l
$(info $(H) Build with debugger information)
endif

#-----------------------------------------------------------------------------
# Target: go build
#-----------------------------------------------------------------------------
.PHONY: build
build: depend baud-server

$(shell chmod 777 ${BAUD_BUILD}/builder.sh)
VER_TO_INT:=awk '{split(substr($$0, match ($$0, /[0-9\.]+/)), a, "."); print a[1]*10000+a[2]*100+a[3]}'
export BAUDSERVER_OUT:=$(BAUD_OUT)/$(GOOS)_$(GOARCH)/$(BUILDTYPE_DIR)
.PHONY: depend
depend:
	@if test $(shell $(GO) version | $(VER_TO_INT) ) -lt \
                 $(shell echo "$(GO_VERSION_REQUIRED)" | $(VER_TO_INT) ); \
                 then printf "go version $(GO_VERSION_REQUIRED)+ required, found: "; $(GO) version; exit 1; fi

.PHONY: baud-server
baud-server:
	${BAUD_BUILD}/builder.sh  ${BAUDPATH}/cmd/server/main.go github.com/tiglabs/baudengine/util/build ${BAUDSERVER_OUT}/baud-server
