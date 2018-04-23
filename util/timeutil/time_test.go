package timeutil

import (
	"testing"
	"time"
)

func TestParseTime(t *testing.T) {
	dt := "2018-01-12 14:08:08"
	edt := time.Date(2018, time.January, 12, 14, 8, 8, 0, time.Local)
	if rs, err := ParseTime(dt); err != nil {
		t.Error(err.Error())
	} else if edt.Nanosecond() != rs.Nanosecond() {
		t.Error("parse time result error.")
	}

	if rs, err := ParseTime(dt, DEFAULT_FORMAT); err != nil {
		t.Error(err.Error())
	} else if edt.Nanosecond() != rs.Nanosecond() {
		t.Error("parse time result error.")
	}
}

func TestFormatTime(t *testing.T) {
	dt := "2018-01-12 14:08:08"
	edt := time.Date(2018, time.January, 12, 14, 8, 8, 0, time.Local)

	if dt != FormatTime(edt) {
		t.Error("format time result error.")
	}

	if dt != FormatTime(edt, DEFAULT_FORMAT) {
		t.Error("format time result error.")
	}

	t.Log(FormatNow())
}
