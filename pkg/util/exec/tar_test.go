package exec

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

const (
	testFileName    = "hello.txt"
	testFileContent = "hello world"
	testDirName     = "subdir"
)

func TestCopyFromPod_EmptyTar(t *testing.T) {
	g := NewWithT(t)

	destDir := t.TempDir()

	var writerClosed atomic.Bool
	executor := &MockExecutor{
		ExecFn: func(_ context.Context, opts ExecOptions) error {
			tw := tar.NewWriter(opts.Stdout)
			_ = tw.Close()

			_, err := opts.Stdout.Write([]byte{0, 0, 0, 0})
			if err != nil {
				writerClosed.Store(true)
			}

			return nil
		},
	}

	err := CopyFromPod(t.Context(), executor, CopyOptions{
		Namespace:     "ns",
		PodName:       "pod",
		ContainerName: "ctr",
		PodPath:       "/data",
		LocalPath:     destDir,
	})

	g.Expect(err).ToNot(HaveOccurred())

	entries, err := os.ReadDir(destDir)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(entries).To(BeEmpty())
}

func TestCopyFromPod_WithFiles(t *testing.T) {
	g := NewWithT(t)

	destDir := t.TempDir()

	executor := &MockExecutor{
		ExecFn: func(_ context.Context, opts ExecOptions) error {
			tw := tar.NewWriter(opts.Stdout)

			content := []byte(testFileContent)
			err := tw.WriteHeader(&tar.Header{
				Name: testFileName,
				Mode: tarFilePermission,
				Size: int64(len(content)),
			})
			if err != nil {
				return err
			}
			if _, err := tw.Write(content); err != nil {
				return err
			}

			return tw.Close()
		},
	}

	err := CopyFromPod(t.Context(), executor, CopyOptions{
		Namespace:     "ns",
		PodName:       "pod",
		ContainerName: "ctr",
		PodPath:       "/data",
		LocalPath:     destDir,
	})

	g.Expect(err).ToNot(HaveOccurred())

	data, err := os.ReadFile(filepath.Join(destDir, testFileName))
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(string(data)).To(Equal(testFileContent))
}

func TestCopyFromPod_ExtractionError(t *testing.T) {
	g := NewWithT(t)

	destDir := t.TempDir()

	executor := &MockExecutor{
		ExecFn: func(_ context.Context, opts ExecOptions) error {
			_, _ = opts.Stdout.Write([]byte("not a valid tar stream"))

			return nil
		},
	}

	err := CopyFromPod(t.Context(), executor, CopyOptions{
		Namespace:     "ns",
		PodName:       "pod",
		ContainerName: "ctr",
		PodPath:       "/data",
		LocalPath:     destDir,
	})

	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("extracting tar from pod"))
}

func TestCopyFromPod_DrainsPipeBeforeClose(t *testing.T) {
	g := NewWithT(t)

	destDir := t.TempDir()

	var pipeWriteErr atomic.Value

	executor := &MockExecutor{
		ExecFn: func(_ context.Context, opts ExecOptions) error {
			tw := tar.NewWriter(opts.Stdout)
			_ = tw.Close()

			time.Sleep(10 * time.Millisecond)

			trailing := bytes.Repeat([]byte{0}, 512)
			_, err := opts.Stdout.Write(trailing)
			pipeWriteErr.Store(err)

			return nil
		},
	}

	err := CopyFromPod(t.Context(), executor, CopyOptions{
		Namespace:     "ns",
		PodName:       "pod",
		ContainerName: "ctr",
		PodPath:       "/data",
		LocalPath:     destDir,
	})

	g.Expect(err).ToNot(HaveOccurred())

	storedErr, ok := pipeWriteErr.Load().(error)
	if ok && storedErr != nil {
		t.Errorf("trailing write after tar close should not fail, got: %v", storedErr)
	}
}

func TestCopyToPod(t *testing.T) {
	g := NewWithT(t)

	srcDir := t.TempDir()

	err := os.MkdirAll(filepath.Join(srcDir, testDirName), tarDirPermission)
	g.Expect(err).ToNot(HaveOccurred())

	err = os.WriteFile(filepath.Join(srcDir, testFileName), []byte(testFileContent), tarFilePermission)
	g.Expect(err).ToNot(HaveOccurred())

	var received bytes.Buffer
	executor := &MockExecutor{
		ExecFn: func(_ context.Context, opts ExecOptions) error {
			_, err := io.Copy(&received, opts.Stdin)

			return err
		},
	}

	err = CopyToPod(t.Context(), executor, CopyOptions{
		Namespace:     "ns",
		PodName:       "pod",
		ContainerName: "ctr",
		PodPath:       "/data",
		LocalPath:     srcDir,
	})

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(received.Len()).To(BeNumerically(">", 0))

	tr := tar.NewReader(&received)
	names := make([]string, 0)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		g.Expect(err).ToNot(HaveOccurred())
		names = append(names, header.Name)
	}

	g.Expect(names).To(ContainElement(testFileName))
	g.Expect(names).To(ContainElement(testDirName))
}

func TestExtractTar_DirectoryTraversal(t *testing.T) {
	g := NewWithT(t)

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	err := tw.WriteHeader(&tar.Header{
		Name: "../../etc/passwd",
		Mode: tarFilePermission,
		Size: 4,
	})
	g.Expect(err).ToNot(HaveOccurred())

	_, err = tw.Write([]byte("evil"))
	g.Expect(err).ToNot(HaveOccurred())

	err = tw.Close()
	g.Expect(err).ToNot(HaveOccurred())

	destDir := t.TempDir()
	err = extractTar(&buf, destDir)

	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("escapes destination directory"))
}
