package build

import (
	"fmt"
	"runtime"
	"strings"
)

// The following fields are populated at buildtime with -ldflags -X.
var (
	AppVersion  = "unknown"
	GitRevision = "unknown"
	User        = "unknown"
	BuiltTime   = "unknown"
	Tag         = "unknown"
	Type        = "unknown"
)

// Info build info
type Info struct {
	AppVersion  string
	GitRevision string
	User        string
	BuiltTime   string
	Tag         string
	Type        string // "release" or "snapshot"...
	GoVersion   string
	Platform    string
}

func (b Info) String() string {
	return fmt.Sprintf(`Version: %v
GitRevision: %v
User: %v
Tag: %v
Type: %v
GoVersion: %v
Platform: %v
BuiltTime: %v
`,
		b.AppVersion,
		b.GitRevision,
		b.User,
		b.Tag,
		b.Type,
		b.GoVersion,
		b.Platform,
		b.BuiltTime)
}

var info Info

func init() {
	info.AppVersion = AppVersion
	info.GitRevision = GitRevision
	info.User = User
	info.BuiltTime = BuiltTime
	info.Tag = Tag
	info.Type = Type
	info.GoVersion = runtime.Version()
	info.Platform = fmt.Sprintf("%s %s", runtime.GOOS, runtime.GOARCH)
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
	return info
}
