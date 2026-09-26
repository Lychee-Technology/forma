package sqlgen

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestMain pins the process zone to Europe/Berlin for every test in this
// package. The filter binders must render an iso8601 literal as the UTC
// image the column stores regardless of the server's zone (#588):
// time.UnixMilli is local, so a rendering that forgot .UTC() only passed on
// a UTC machine, which is what CI runs. A test that needs UTC builds it
// explicitly with time.UTC.
func TestMain(m *testing.M) {
	loc, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		fmt.Fprintf(os.Stderr, "sqlgen tests need the Europe/Berlin zone: %v\n", err)
		os.Exit(1)
	}
	time.Local = loc
	os.Exit(m.Run())
}
