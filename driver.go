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
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"time"
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
	r, err := git.PlainOpen(driver.RootPath)
	if err != nil {
		return err
	}
	tree, err := r.Worktree()
	if err != nil {
		return err
	}

	f, err := tree.Filesystem.Stat(path)
	if err != nil {
		return err
	}
	if f.IsDir() {
		return nil
	}
	return errors.New("Not a directory")
}

func (driver *GitDriver) Stat(path string) (server.FileInfo, error) {
	r, err := git.PlainOpen(driver.RootPath)
	if err != nil {
		return nil, err
	}
	tree, err := r.Worktree()
	if err != nil {
		return nil, err
	}

	f, err := tree.Filesystem.Stat(path)
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
	r, err := git.PlainOpen(driver.RootPath)
	if err != nil {
		return err
	}

	tree, err := r.Worktree()
	if err != nil {
		return err
	}

	files, err := tree.Filesystem.ReadDir(path)
	if err != nil {
		return err
	}

	paths := strings.Split(path, "/")

	for _, file := range files {
		rPath := filepath.Join(append(paths, file.Name())...)

		info, err := tree.Filesystem.Stat(rPath)
		if err != nil {
			return err
		}

		mode := info.Mode()
		if info.IsDir() {
			mode |= os.ModeDir
		}

		owner, err := driver.Perm.GetOwner(path)
		if err != nil {
			return err
		}
		group, err := driver.Perm.GetGroup(path)
		if err != nil {
			return err
		}

		err = callback(&FileInfo{info, mode, owner, group})
		if err != nil {
			return err
		}
	}

	return nil
}

func (driver *GitDriver) DeleteDir(path string) error {
	r, err := git.PlainOpen(driver.RootPath)
	if err != nil {
		return err
	}
	tree, err := r.Worktree()
	if err != nil {
		return err
	}

	f, err := tree.Filesystem.Lstat(path)
	if err != nil {
		return err
	}

	if !f.IsDir() {
		return errors.New("Not a directory")
	}

	err = tree.Filesystem.Remove(path)
	if err != nil {
		return err
	}

	err = driver.add(path, r)
	if err != nil {
		return err
	}

	return driver.commit(r)
}

func (driver *GitDriver) DeleteFile(path string) error {
	r, err := git.PlainOpen(driver.RootPath)
	if err != nil {
		return err
	}
	tree, err := r.Worktree()
	if err != nil {
		return err
	}

	f, err := tree.Filesystem.Lstat(path)
	if err != nil {
		return err
	}

	if f.IsDir() {
		return errors.New("Not a file")
	}

	err = tree.Filesystem.Remove(path)
	if err != nil {
		return err
	}

	err = driver.add(path, r)
	if err != nil {
		return err
	}

	return driver.commit(r)
}

func (driver *GitDriver) Rename(fromPath string, toPath string) error {
	r, err := git.PlainOpen(driver.RootPath)
	if err != nil {
		return err
	}
	tree, err := r.Worktree()
	if err != nil {
		return err
	}

	err = tree.Filesystem.Rename(fromPath, toPath)
	if err != nil {
		return err
	}

	err = driver.add(fromPath, r)
	if err != nil {
		return err
	}
	err = driver.add(toPath, r)
	if err != nil {
		return err
	}

	return driver.commit(r)
}

func (driver *GitDriver) MakeDir(path string) error {
	r, err := git.PlainOpen(driver.RootPath)
	if err != nil {
		return err
	}
	tree, err := r.Worktree()
	if err != nil {
		return err
	}

	err = tree.Filesystem.MkdirAll(path, os.ModePerm)
	if err != nil {
		return err
	}

	err = driver.add(path, r)
	if err != nil {
		return err
	}

	return driver.commit(r)
}

func (driver *GitDriver) GetFile(path string, offset int64) (int64, io.ReadCloser, error) {
	r, err := git.PlainOpen(driver.RootPath)
	if err != nil {
		return 0, nil, err
	}
	tree, err := r.Worktree()
	if err != nil {
		return 0, nil, err
	}

	info, err := tree.Filesystem.Stat(path)
	if err != nil {
		return 0, nil, err
	}

	f, err := tree.Filesystem.Open(path)
	if err != nil {
		return 0, nil, err
	}

	f.Seek(offset, os.SEEK_SET)

	return info.Size(), f, nil
}

func (driver *GitDriver) add(destPath string, r *git.Repository) error {
	tree, err := r.Worktree()
	if err != nil {
		return err
	}

	addPath, err := filepath.Rel("/", destPath)
	if err != nil {
		return err
	}

	_, err = tree.Add(addPath)
	if err != nil {
		return err
	}

	return nil
}

func (driver *GitDriver) commit(r *git.Repository) error {
	tree, err := r.Worktree()
	if err != nil {
		return err
	}

	_, err = tree.Commit("example go-git commit", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "John Doe",
			Email: "john@doe.org",
			When:  time.Now(),
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func (driver *GitDriver) PutFile(destPath string, data io.Reader, appendData bool) (int64, error) {
	r, err := git.PlainOpen(driver.RootPath)
	if err != nil {
		return 0, err
	}
	tree, err := r.Worktree()
	if err != nil {
		return 0, err
	}

	var isExist bool
	f, err := tree.Filesystem.Lstat(destPath)
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
			err = tree.Filesystem.Remove(destPath)
			if err != nil {
				return 0, err
			}
		}
		f, err := tree.Filesystem.Create(destPath)
		if err != nil {
			return 0, err
		}
		defer f.Close()
		bytes, err := io.Copy(f, data)
		if err != nil {
			return 0, err
		}

		err = driver.add(destPath, r)
		if err != nil {
			return 0, err
		}

		err = driver.commit(r)
		if err != nil {
			return 0, err
		}

		return bytes, nil
	}

	of, err := tree.Filesystem.OpenFile(destPath, os.O_APPEND|os.O_RDWR, 0660)
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

	err = driver.add(destPath, r)
	if err != nil {
		return 0, err
	}

	err = driver.commit(r)
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
