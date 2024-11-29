package implementation_test

import (
	. "github.com/Phosmachina/FluentKV/implementation"
	"testing"
)

func TestBadger(t *testing.T) {
	NewImplementationTester(t).
		SetSetUp(func(i *ImplementationTester) {
			db, _ := NewBadgerDB(t.TempDir())
			i.SetDb(db)
		}).
		RunAllTests()
}
