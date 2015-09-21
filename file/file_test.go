package file

import (
	"github.com/jadeydi/migrate/migrate/direction"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestParseFilenameSchema(t *testing.T) {
	var tests = []struct {
		filename          string
		filenameExtension string
		expectVersion     uint64
		expectName        string
		expectDirection   direction.Direction
		expectErr         bool
	}{
		{"20150907140703_test_file.up.sql", "sql", 20150907140703, "test_file", direction.Up, false},
		{"20150907140703_test_file.down.sql", "sql", 20150907140703, "test_file", direction.Down, false},
		{"20150907150703_test_file.down.sql", "sql", 20150907150703, "test_file", direction.Down, false},
		{"-1_test_file.down.sql", "sql", 0, "", direction.Up, true},
		{"test_file.down.sql", "sql", 0, "", direction.Up, true},
		{"100_test_file.down", "sql", 0, "", direction.Up, true},
		{"100_test_file.sql", "sql", 0, "", direction.Up, true},
		{"100_test_file", "sql", 0, "", direction.Up, true},
		{"test_file", "sql", 0, "", direction.Up, true},
		{"100", "sql", 0, "", direction.Up, true},
		{".sql", "sql", 0, "", direction.Up, true},
		{"up.sql", "sql", 0, "", direction.Up, true},
		{"down.sql", "sql", 0, "", direction.Up, true},
	}

	for _, test := range tests {
		version, name, migrate, err := parseFilenameSchema(test.filename, FilenameRegex(test.filenameExtension))
		if test.expectErr && err == nil {
			t.Fatal("Expected error, but got none.", test)
		}
		if !test.expectErr && err != nil {
			t.Fatal("Did not expect error, but got one:", err, test)
		}
		if err == nil {
			if version != test.expectVersion {
				t.Error("Wrong version number", test)
			}
			if name != test.expectName {
				t.Error("wrong name", test)
			}
			if migrate != test.expectDirection {
				t.Error("wrong migrate", test)
			}
		}
	}
}

func TestFiles(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "TestLookForMigrationFilesInSearchPath")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	if err := ioutil.WriteFile(path.Join(tmpdir, "nonsense.txt"), nil, 0755); err != nil {
		t.Fatal("Unable to write files in tmpdir", err)
	}
	ioutil.WriteFile(path.Join(tmpdir, "002_migrationfile.up.sql"), nil, 0755)
	ioutil.WriteFile(path.Join(tmpdir, "002_migrationfile.down.sql"), nil, 0755)

	ioutil.WriteFile(path.Join(tmpdir, "001_migrationfile.up.sql"), nil, 0755)
	ioutil.WriteFile(path.Join(tmpdir, "001_migrationfile.down.sql"), nil, 0755)

	ioutil.WriteFile(path.Join(tmpdir, "101_create_table.up.sql"), nil, 0755)
	ioutil.WriteFile(path.Join(tmpdir, "101_drop_tables.down.sql"), nil, 0755)

	ioutil.WriteFile(path.Join(tmpdir, "301_migrationfile.up.sql"), nil, 0755)
	ioutil.WriteFile(path.Join(tmpdir, "301_migrationfile.down.sql"), nil, 0755)

	ioutil.WriteFile(path.Join(tmpdir, "401_migrationfile.up.sql"), []byte("test"), 0755)
	ioutil.WriteFile(path.Join(tmpdir, "401_migrationfile.down.sql"), []byte("test"), 0755)

	files, err := ReadMigrationFiles(tmpdir, FilenameRegex("sql"))
	if err != nil {
		t.Fatal(err)
	}

	if len(files) == 0 {
		t.Fatal("No files returned.")
	}

	if len(files) != 5 {
		t.Fatal("Wrong number of files returned.")
	}

	// test sort order
	if files[0].Version != 1 || files[1].Version != 2 || files[2].Version != 101 || files[3].Version != 301 || files[4].Version != 401 {
		t.Error("Sort order is incorrect")
		t.Error(files)
	}

	// test UpFile and DownFile
	if files[0].UpFile == nil {
		t.Fatalf("Missing up file for version %v", files[0].Version)
	}
	if files[0].DownFile == nil {
		t.Fatalf("Missing down file for version %v", files[0].Version)
	}

	if files[1].UpFile == nil {
		t.Fatalf("Missing up file for version %v", files[1].Version)
	}
	if files[1].DownFile == nil {
		t.Fatalf("Missing down file for version %v", files[1].Version)
	}

	if files[2].UpFile == nil {
		t.Fatalf("Missing up file for version %v", files[2].Version)
	}
	if files[2].DownFile == nil {
		t.Fatalf("Missing down file for version %v", files[2].Version)
	}

	if files[3].UpFile == nil {
		t.Fatalf("Missing up file for version %v", files[3].Version)
	}

	if files[4].DownFile == nil {
		t.Fatalf("Missing down file for version %v", files[4].Version)
	}

	// test read
	if err := files[4].DownFile.ReadContent(); err != nil {
		t.Error("Unable to read file", err)
	}
	if files[4].DownFile.Content == nil {
		t.Fatal("Read content is nil")
	}
	if string(files[4].DownFile.Content) != "test" {
		t.Fatal("Read content is wrong")
	}

	// test names
	if files[0].UpFile.Name != "migrationfile" {
		t.Error("file name is not correct", files[0].UpFile.Name)
	}
	if files[0].UpFile.FileName != "001_migrationfile.up.sql" {
		t.Error("file name is not correct", files[0].UpFile.FileName)
	}

	/**
	// test file.From()
	// there should be the following versions:
	// 1(up&down), 2(up&down), 101(up&down), 301(up), 401(down)
	var tests = []struct {
		from        uint64
		relative    int
		expectRange []uint64
	}{
		{0, 2, []uint64{1, 2}},
		{1, 4, []uint64{2, 101, 301}},
		{1, 0, nil},
		{0, 1, []uint64{1}},
		{0, 0, nil},
		{101, -2, []uint64{101, 2}},
		{401, -1, []uint64{401}},
	}

	for _, test := range tests {
		rangeFiles, err := files.From(test.from, test.relative)
		if err != nil {
			t.Error("Unable to fetch range:", err)
		}
		if len(rangeFiles) != len(test.expectRange) {
			t.Fatalf("file.From(): expected %v files, got %v. For test %v.", len(test.expectRange), len(rangeFiles), test.expectRange)
		}

		for i, version := range test.expectRange {
			if rangeFiles[i].Version != version {
				t.Fatal("file.From(): returned files dont match expectations", test.expectRange)
			}
		}
	}
	**/

	// test ToFirstFrom
	tffFiles, err := files.ToFirstFrom(map[uint64]bool{101: true, 401: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(tffFiles) != 2 {
		t.Fatalf("Wrong number of files returned by ToFirstFrom(), expected %v, got %v.", 1, len(tffFiles))
	}
	if tffFiles[0].Direction != direction.Down {
		t.Error("ToFirstFrom() did not return DownFiles")
	}

	// test ToLastFrom
	tofFiles, err := files.ToLastFrom(map[uint64]bool{101: true, 401: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(tofFiles) != 3 {
		t.Fatalf("Wrong number of files returned by ToLastFrom(), expected %v, got %v.", 3, len(tofFiles))
	}
	if tofFiles[0].Direction != direction.Up {
		t.Error("ToFirstFrom() did not return UpFiles")
	}

}
