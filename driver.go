package giftp

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/goftp/server"
	"gopkg.in/src-d/go-git.v4"
)

type GitDriver struct {
	RootPath string
	server.Perm
}

type FileInfo struct {
	os.FileInfo

	mode  os.FileMode
	owner string
	group string
}

func (f *FileInfo) Mode() os.FileMode {
	return f.mode
}

func (f *FileInfo) Owner() string {
	return f.owner
}

func (f *FileInfo) Group() string {
	return f.group
}

func (driver *GitDriver) realPath(path string) string {
	paths := strings.Split(path, "/")
	return filepath.Join(append([]string{driver.RootPath}, paths...)...)
}

func (driver *GitDriver) Init(conn *server.Conn) {
}

func (driver *GitDriver) ChangeDir(path string) error {
	rPath := driver.realPath(path)
	f, err := os.Lstat(rPath)
	if err != nil {
		return err
	}
	if f.IsDir() {
		return nil
	}
	return errors.New("Not a directory")
}

func (driver *GitDriver) Stat(path string) (server.FileInfo, error) {
	basepath := driver.realPath(path)
	rPath, err := filepath.Abs(basepath)
	if err != nil {
		return nil, err
	}
	f, err := os.Lstat(rPath)
	if err != nil {
		return nil, err
	}
	mode, err := driver.Perm.GetMode(path)
	if err != nil {
		return nil, err
	}
	if f.IsDir() {
		mode |= os.ModeDir
	}
	owner, err := driver.Perm.GetOwner(path)
	if err != nil {
		return nil, err
	}
	group, err := driver.Perm.GetGroup(path)
	if err != nil {
		return nil, err
	}
	return &FileInfo{f, mode, owner, group}, nil
}

func (driver *GitDriver) ListDir(path string, callback func(server.FileInfo) error) error {
	fmt.Println(driver.RootPath)

	r, err := git.PlainOpen(driver.RootPath)
	if err != nil {
		return err
	}

	tree, err := r.Worktree()
	if err != nil {
		return err
	}

	fmt.Println(path)
	files, err := tree.Filesystem.ReadDir(path)
	if err != nil {
		return err
	}

	for _, file := range files {
		info, err := tree.Filesystem.Stat(file.Name())
		owner := "nobody"
		group := "nobody"

		mode := info.Mode()
		if info.IsDir() {
			mode |= os.ModeDir
		}

		err = callback(&FileInfo{info, mode, owner, group})
		if err != nil {
			return err
		}
	}

	return nil
}

func (driver *GitDriver) DeleteDir(path string) error {
	rPath := driver.realPath(path)
	f, err := os.Lstat(rPath)
	if err != nil {
		return err
	}
	if f.IsDir() {
		return os.Remove(rPath)
	}
	return errors.New("Not a directory")
}

func (driver *GitDriver) DeleteFile(path string) error {
	rPath := driver.realPath(path)
	f, err := os.Lstat(rPath)
	if err != nil {
		return err
	}
	if !f.IsDir() {
		return os.Remove(rPath)
	}
	return errors.New("Not a file")
}

func (driver *GitDriver) Rename(fromPath string, toPath string) error {
	oldPath := driver.realPath(fromPath)
	newPath := driver.realPath(toPath)
	return os.Rename(oldPath, newPath)
}

func (driver *GitDriver) MakeDir(path string) error {
	rPath := driver.realPath(path)
	return os.MkdirAll(rPath, os.ModePerm)
}

func (driver *GitDriver) GetFile(path string, offset int64) (int64, io.ReadCloser, error) {
	rPath := driver.realPath(path)
	f, err := os.Open(rPath)
	if err != nil {
		return 0, nil, err
	}

	info, err := f.Stat()
	if err != nil {
		return 0, nil, err
	}

	f.Seek(offset, os.SEEK_SET)

	return info.Size(), f, nil
}

func (driver *GitDriver) PutFile(destPath string, data io.Reader, appendData bool) (int64, error) {
	rPath := driver.realPath(destPath)
	var isExist bool
	f, err := os.Lstat(rPath)
	if err == nil {
		isExist = true
		if f.IsDir() {
			return 0, errors.New("A dir has the same name")
		}
	} else {
		if os.IsNotExist(err) {
			isExist = false
		} else {
			return 0, errors.New(fmt.Sprintln("Put File error:", err))
		}
	}

	if appendData && !isExist {
		appendData = false
	}

	if !appendData {
		if isExist {
			err = os.Remove(rPath)
			if err != nil {
				return 0, err
			}
		}
		f, err := os.Create(rPath)
		if err != nil {
			return 0, err
		}
		defer f.Close()
		bytes, err := io.Copy(f, data)
		if err != nil {
			return 0, err
		}
		return bytes, nil
	}

	of, err := os.OpenFile(rPath, os.O_APPEND|os.O_RDWR, 0660)
	if err != nil {
		return 0, err
	}
	defer of.Close()

	_, err = of.Seek(0, os.SEEK_END)
	if err != nil {
		return 0, err
	}

	bytes, err := io.Copy(of, data)
	if err != nil {
		return 0, err
	}

	return bytes, nil
}

type GitDriverFactory struct {
	RootPath string
	server.Perm
}

func (factory *GitDriverFactory) NewDriver() (server.Driver, error) {
	return &GitDriver{factory.RootPath, factory.Perm}, nil
}
