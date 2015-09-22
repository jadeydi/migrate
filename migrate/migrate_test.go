package migrate

import (
	"io/ioutil"
	"testing"
	"time"
)

// Add Driver URLs here to test basic Up, Down, .. functions.
var driverUrls = []string{
	"postgres://root@localhost:5432/migratetest?sslmode=disable",
}

func TestCreate(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		if _, err := Create(driverUrl, tmpdir, "test_migration"); err != nil {
			t.Fatal(err)
		}

		files, err := ioutil.ReadDir(tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if len(files) != 2 {
			t.Fatal("Expected 2 new files, got", len(files))
		}
		for _, file := range files {
			t.Log(file.Name())
		}
	}
}

func TestDown(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		Create(driverUrl, tmpdir, "migration1")
		time.Sleep(time.Second * 2)
		Create(driverUrl, tmpdir, "migration2")

		errs, ok := ResetSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if len(version) != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}

		errs, ok = DownSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if len(version) != 0 {
			t.Fatalf("Expected version 0, got %v", version)
		}
	}
}

func TestUp(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		Create(driverUrl, tmpdir, "migration1")
		time.Sleep(time.Second * 2)
		Create(driverUrl, tmpdir, "migration2")

		errs, ok := DownSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if len(version) != 0 {
			t.Fatalf("Expected version 0, got %v", version)
		}

		errs, ok = UpSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if len(version) != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}
		DownSync(driverUrl, tmpdir)
	}
}

/**
func TestRedo(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		Create(driverUrl, tmpdir, "migration1")
		Create(driverUrl, tmpdir, "migration2")

		errs, ok := ResetSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}

		errs, ok = RedoSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}
	}
}

func TestMigrate(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		Create(driverUrl, tmpdir, "migration1")
		Create(driverUrl, tmpdir, "migration2")

		errs, ok := ResetSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}

		errs, ok = MigrateSync(driverUrl, tmpdir, -2)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != 0 {
			t.Fatalf("Expected version 0, got %v", version)
		}

		errs, ok = MigrateSync(driverUrl, tmpdir, +1)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != 1 {
			t.Fatalf("Expected version 1, got %v", version)
		}
	}
}
**/
