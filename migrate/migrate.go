// Package migrate is imported by other Go code.
// It is the entry point to all migration functions.
package migrate

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/jadeydi/migrate/driver"
	"github.com/jadeydi/migrate/file"
	"github.com/jadeydi/migrate/migrate/direction"
	pipep "github.com/jadeydi/migrate/pipe"
)

// Up applies all available migrations
func Up(pipe chan interface{}, url, migrationsPath string) {
	// 初始的version是0
	d, files, version, err := initDriverAndReadMigrationFilesAndGetVersion(url, migrationsPath)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	// TODO:// 这个实现不适合应该是从数据库里所有的都匹配
	applyMigrationFiles, err := files.ToLastFrom(version)
	if err != nil {
		if err2 := d.Close(); err2 != nil {
			pipe <- err2
		}
		go pipep.Close(pipe, err)
		return
	}

	if len(applyMigrationFiles) > 0 {
		for _, f := range applyMigrationFiles {
			pipe1 := pipep.New()
			go d.Migrate(f, pipe1)
			if ok := pipep.WaitAndRedirect(pipe1, pipe, handleInterrupts()); !ok {
				break
			}
		}
		if err := d.Close(); err != nil {
			pipe <- err
		}
		go pipep.Close(pipe, nil)
		return
	} else {

		if err := d.Close(); err != nil {
			pipe <- err
		}
		go pipep.Close(pipe, nil)
		return
	}
}

// UpSync is synchronous version of Up
func UpSync(url, migrationsPath string) (err []error, ok bool) {
	pipe := pipep.New()
	go Up(pipe, url, migrationsPath)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Down rolls back all migrations
func Down(pipe chan interface{}, url, migrationsPath string) {
	d, files, version, err := initDriverAndReadMigrationFilesAndGetVersion(url, migrationsPath)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	applyMigrationFiles, err := files.ToFirstFrom(version)
	if err != nil {
		if err2 := d.Close(); err2 != nil {
			pipe <- err2
		}
		go pipep.Close(pipe, err)
		return
	}

	if len(applyMigrationFiles) > 0 {
		for _, f := range applyMigrationFiles {
			pipe1 := pipep.New()
			go d.Migrate(f, pipe1)
			if ok := pipep.WaitAndRedirect(pipe1, pipe, handleInterrupts()); !ok {
				break
			}
		}
		if err2 := d.Close(); err2 != nil {
			pipe <- err2
		}
		go pipep.Close(pipe, nil)
		return
	} else {
		if err2 := d.Close(); err2 != nil {
			pipe <- err2
		}
		go pipep.Close(pipe, nil)
		return
	}
}

// DownSync is synchronous version of Down
func DownSync(url, migrationsPath string) (err []error, ok bool) {
	pipe := pipep.New()
	go Down(pipe, url, migrationsPath)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Redo rolls back the most recently applied migration, then runs it again.
func Redo(pipe chan interface{}, url, migrationsPath string) {
	d, file, err := getRedoFile(url, migrationsPath)

	if err != nil || file == nil {
		go pipep.Close(pipe, nil)
		return
	}
	pipe1 := pipep.New()
	go d.Migrate(*file.DownFile, pipe1)
	if ok := pipep.WaitAndRedirect(pipe1, pipe, handleInterrupts()); !ok {
		go pipep.Close(pipe, nil)
		return
	} else {
		go d.Migrate(*file.UpFile, pipe)
	}
}

// RedoSync is synchronous version of Redo
func RedoSync(url, migrationsPath string) (err []error, ok bool) {
	pipe := pipep.New()
	go Redo(pipe, url, migrationsPath)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Reset runs the down and up migration function
func Reset(pipe chan interface{}, url, migrationsPath string) {
	pipe1 := pipep.New()
	go Down(pipe1, url, migrationsPath)
	if ok := pipep.WaitAndRedirect(pipe1, pipe, handleInterrupts()); !ok {
		go pipep.Close(pipe, nil)
		return
	} else {
		go Up(pipe, url, migrationsPath)
	}
}

// ResetSync is synchronous version of Reset
func ResetSync(url, migrationsPath string) (err []error, ok bool) {
	pipe := pipep.New()
	go Reset(pipe, url, migrationsPath)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Migrate applies relative +n/-n migrations
func Migrate(pipe chan interface{}, url, migrationsPath string, relativeN int) {
	d, files, version, err := initDriverAndReadMigrationFilesAndGetVersion(url, migrationsPath)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	applyMigrationFiles, err := files.From(version, relativeN)
	if err != nil {
		if err2 := d.Close(); err2 != nil {
			pipe <- err2
		}
		go pipep.Close(pipe, err)
		return
	}

	if len(applyMigrationFiles) > 0 && relativeN != 0 {
		for _, f := range applyMigrationFiles {
			pipe1 := pipep.New()
			go d.Migrate(f, pipe1)
			if ok := pipep.WaitAndRedirect(pipe1, pipe, handleInterrupts()); !ok {
				break
			}
		}
		if err2 := d.Close(); err2 != nil {
			pipe <- err2
		}
		go pipep.Close(pipe, nil)
		return
	}
	if err2 := d.Close(); err2 != nil {
		pipe <- err2
	}
	go pipep.Close(pipe, nil)
	return
}

// MigrateSync is synchronous version of Migrate
func MigrateSync(url, migrationsPath string, relativeN int) (err []error, ok bool) {
	pipe := pipep.New()
	go Migrate(pipe, url, migrationsPath, relativeN)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Version returns the current migration version
func Version(url, migrationsPath string) (version uint64, err error) {
	d, err := driver.New(url)
	if err != nil {
		return 0, err
	}
	return d.Version()
}

func Versions(url, migrationsPath string) (versions map[uint64]bool, err error) {
	d, err := driver.New(url)
	if err != nil {
		return map[uint64]bool{}, err
	}
	return d.Versions()
}

// Create creates new migration files on disk
func Create(url, migrationsPath, name string) (*file.MigrationFile, error) {
	d, err := driver.New(url)
	if err != nil {
		return nil, err
	}

	versionStr := time.Now().Format("20060102150405")
	version, _ := strconv.ParseUint(versionStr, 10, 64)
	filenamef := "%s_%s.%s.%s"
	name = strings.Replace(name, " ", "_", -1)

	mfile := &file.MigrationFile{
		Version: version,
		UpFile: &file.File{
			Path:      migrationsPath,
			FileName:  fmt.Sprintf(filenamef, versionStr, name, "up", d.FilenameExtension()),
			Name:      name,
			Content:   []byte(""),
			Direction: direction.Up,
		},
		DownFile: &file.File{
			Path:      migrationsPath,
			FileName:  fmt.Sprintf(filenamef, versionStr, name, "down", d.FilenameExtension()),
			Name:      name,
			Content:   []byte(""),
			Direction: direction.Down,
		},
	}

	if err := ioutil.WriteFile(path.Join(mfile.UpFile.Path, mfile.UpFile.FileName), mfile.UpFile.Content, 0644); err != nil {
		return nil, err
	}
	if err := ioutil.WriteFile(path.Join(mfile.DownFile.Path, mfile.DownFile.FileName), mfile.DownFile.Content, 0644); err != nil {
		return nil, err
	}

	return mfile, nil
}

// initDriverAndReadMigrationFilesAndGetVersion is a small helper
// function that is common to most of the migration funcs
func initDriverAndReadMigrationFilesAndGetVersion(url, migrationsPath string) (driver.Driver, *file.MigrationFiles, map[uint64]bool, error) {
	d, err := driver.New(url)
	if err != nil {
		return nil, nil, map[uint64]bool{}, err
	}
	// FilenameExtension 返回 "sh" string
	// file.FilenameRegex(d.FilenameExtension()) 是 Regex 的指针
	// 返回的就是所有的 files 数组
	files, err := file.ReadMigrationFiles(migrationsPath, file.FilenameRegex(d.FilenameExtension()))
	if err != nil {
		d.Close() // TODO what happens with errors from this func?
		return nil, nil, map[uint64]bool{}, err
	}
	versions, err := d.Versions()
	if err != nil {
		d.Close() // TODO what happens with errors from this func?
		return nil, nil, map[uint64]bool{}, err
	}
	return d, &files, versions, nil
}

func getRedoFile(url, migrationsPath string) (driver.Driver, *file.MigrationFile, error) {
	d, err := driver.New(url)
	if err != nil {
		d.Close()
		return nil, nil, err
	}
	version, err := d.Version()
	if err != nil {
		d.Close()
		return nil, nil, err
	}
	if version == 0 {
		d.Close()
		return nil, nil, nil
	}
	file, err := file.ReadFileByVersion(version, migrationsPath, file.FilenameRegex(d.FilenameExtension()))
	if err != nil {
		d.Close()
		return nil, nil, err
	}

	return d, file, nil
}

// NewPipe is a convenience function for pipe.New().
// This is helpful if the user just wants to import this package and nothing else.
func NewPipe() chan interface{} {
	return pipep.New()
}

// interrupts is an internal variable that holds the state of
// interrupt handling
var interrupts = true

// Graceful enables interrupts checking. Once the first ^C is received
// it will finish the currently running migration and abort execution
// of the next migration. If ^C is received twice, it will stop
// execution immediately.
func Graceful() {
	interrupts = true
}

// NonGraceful disables interrupts checking. The first received ^C will
// stop execution immediately.
func NonGraceful() {
	interrupts = false
}

// interrupts returns a signal channel if interrupts checking is
// enabled. nil otherwise.
func handleInterrupts() chan os.Signal {
	if interrupts {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		return c
	}
	return nil
}
