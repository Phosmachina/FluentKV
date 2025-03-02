package driver_test

import (
	. "github.com/Phosmachina/FluentKV/driver"
	"testing"
)

func TestBadger(t *testing.T) {
	NewDriverTester(t).
		SetSetUp(func(i *DriverTester) {
			db, _ := NewBadgerDB(t.TempDir())
			i.SetDb(db)
		}).
		RunAllTests()
}
