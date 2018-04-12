package build

import (
	"fmt"
	"runtime"
	"strings"
)

// The following fields are populated at buildtime with -ldflags -X.
var (
	AppName     = "unknown"
	AppVersion  = "unknown"
	GitRevision = "unknown"
	User        = "unknown"
	BuiltTime   = "unknown"
	Tag         = "unknown"
	Type        = "unknown"
)

// Info build info
type Info struct {
	AppName     string
	AppVersion  string
	GitRevision string
	User        string
	BuiltTime   string
	Tag         string
	Type        string // "release" or "snapshot"...
	GoVersion   string
	Platform    string
}

// NewInfo create info object
func NewInfo() *Info {
	return &Info{
		AppName:     AppName,
		AppVersion:  AppVersion,
		GitRevision: GitRevision,
		User:        User,
		BuiltTime:   BuiltTime,
		Tag:         Tag,
		Type:        Type,
		GoVersion:   runtime.Version(),
		Platform:    fmt.Sprintf("%s %s", runtime.GOOS, runtime.GOARCH),
	}
}

func (b *Info) String() string {
	return fmt.Sprintf(`
App: %v
Version: %v
GitRevision: %v
User: %v
Tag: %v
Type: %v
GoVersion: %v
Platform: %v
BuiltTime: %v
`,
		b.AppName,
		b.AppVersion,
		b.GitRevision,
		b.User,
		b.Tag,
		b.Type,
		b.GoVersion,
		b.Platform,
		b.BuiltTime)
}

var info *Info

func init() {
	info = NewInfo()
}

// Version returns a multi-line version information
func Version() string {
	return info.String()
}

// IsRelease return build is release verison
func IsRelease() bool {
	return strings.HasPrefix(info.Type, "release")
}

// GetInfo return build info
func GetInfo() Info {
	return *info
}
