package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	_ "time"
)

func TestLocalStorage_Upload(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建本地存储实例
	storage := NewLocalStorage(LocalStorageConfig{
		BasePath: tempDir,
	})

	// 测试上传文件
	content := "Hello, World!"
	reader := bytes.NewReader([]byte(content))
	filePath := "test.txt"

	err = storage.Upload(context.Background(), filePath, reader)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	// 验证文件是否存在
	fullPath := filepath.Join(tempDir, filePath)
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		t.Fatal("File was not created")
	}

	// 验证文件内容
	data, err := os.ReadFile(fullPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	if string(data) != content {
		t.Fatalf("File content mismatch. Expected: %s, Got: %s", content, string(data))
	}
}

func TestLocalStorage_Download(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建本地存储实例
	storage := NewLocalStorage(LocalStorageConfig{
		BasePath: tempDir,
	})

	// 创建测试文件
	content := "Hello, World!"
	filePath := "test.txt"
	fullPath := filepath.Join(tempDir, filePath)

	err = os.WriteFile(fullPath, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// 测试下载文件
	reader, err := storage.Download(context.Background(), filePath)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	// 验证下载内容
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read downloaded content: %v", err)
	}

	if string(data) != content {
		t.Fatalf("Downloaded content mismatch. Expected: %s, Got: %s", content, string(data))
	}
}

func TestLocalStorage_Delete(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建本地存储实例
	storage := NewLocalStorage(LocalStorageConfig{
		BasePath: tempDir,
	})

	// 创建测试文件
	content := "Hello, World!"
	filePath := "test.txt"
	fullPath := filepath.Join(tempDir, filePath)

	err = os.WriteFile(fullPath, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// 确保文件存在
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		t.Fatal("Test file was not created")
	}

	// 测试删除文件
	err = storage.Delete(context.Background(), filePath)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 验证文件是否被删除
	if _, err := os.Stat(fullPath); !os.IsNotExist(err) {
		t.Fatal("File was not deleted")
	}
}

func TestLocalStorage_Rename(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建本地存储实例
	storage := NewLocalStorage(LocalStorageConfig{
		BasePath: tempDir,
	})

	// 创建测试文件
	content := "Hello, World!"
	oldPath := "old.txt"
	newPath := "new.txt"
	oldFullPath := filepath.Join(tempDir, oldPath)

	err = os.WriteFile(oldFullPath, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// 确保原文件存在
	if _, err := os.Stat(oldFullPath); os.IsNotExist(err) {
		t.Fatal("Test file was not created")
	}

	// 测试重命名文件
	err = storage.Rename(context.Background(), oldPath, newPath)
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// 验证原文件是否不存在
	if _, err := os.Stat(oldFullPath); !os.IsNotExist(err) {
		t.Fatal("Old file still exists")
	}

	// 验证新文件是否存在
	newFullPath := filepath.Join(tempDir, newPath)
	if _, err := os.Stat(newFullPath); os.IsNotExist(err) {
		t.Fatal("New file was not created")
	}

	// 验证新文件内容
	data, err := os.ReadFile(newFullPath)
	if err != nil {
		t.Fatalf("Failed to read new file: %v", err)
	}

	if string(data) != content {
		t.Fatalf("New file content mismatch. Expected: %s, Got: %s", content, string(data))
	}
}

func TestLocalStorage_CreateDir(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建本地存储实例
	storage := NewLocalStorage(LocalStorageConfig{
		BasePath: tempDir,
	})

	// 测试创建目录
	dirPath := "testdir"
	err = storage.CreateDir(context.Background(), dirPath)
	if err != nil {
		t.Fatalf("CreateDir failed: %v", err)
	}

	// 验证目录是否存在
	fullPath := filepath.Join(tempDir, dirPath)
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		t.Fatal("Directory was not created")
	}

	// 验证是否为目录
	info, err := os.Stat(fullPath)
	if err != nil {
		t.Fatalf("Failed to get directory info: %v", err)
	}

	if !info.IsDir() {
		t.Fatal("Created path is not a directory")
	}
}

func TestLocalStorage_GetMetadata(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建本地存储实例
	storage := NewLocalStorage(LocalStorageConfig{
		BasePath: tempDir,
	})

	// 创建测试文件
	content := "Hello, World!"
	filePath := "test.txt"
	fullPath := filepath.Join(tempDir, filePath)

	err = os.WriteFile(fullPath, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// 获取文件信息用于比较
	expectedInfo, err := os.Stat(fullPath)
	if err != nil {
		t.Fatalf("Failed to get test file info: %v", err)
	}

	// 测试获取元数据
	metadata, err := storage.GetMetadata(context.Background(), filePath)
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}

	// 验证元数据
	if metadata.Name != filePath {
		t.Fatalf("Metadata name mismatch. Expected: %s, Got: %s", filePath, metadata.Name)
	}

	if metadata.Size != expectedInfo.Size() {
		t.Fatalf("Metadata size mismatch. Expected: %d, Got: %d", expectedInfo.Size(), metadata.Size)
	}

	// 允许1秒的时间差
	if metadata.ModTime.Sub(expectedInfo.ModTime()).Seconds() > 1 {
		t.Fatalf("Metadata mod time mismatch. Expected: %v, Got: %v", expectedInfo.ModTime(), metadata.ModTime)
	}

	if metadata.IsDir != expectedInfo.IsDir() {
		t.Fatalf("Metadata isDir mismatch. Expected: %t, Got: %t", expectedInfo.IsDir(), metadata.IsDir)
	}
}

func TestLocalStorage_BatchUpload(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建本地存储实例
	storage := NewLocalStorage(LocalStorageConfig{
		BasePath: tempDir,
	})

	// 准备批量上传的文件
	files := map[string]io.Reader{
		"file1.txt":     bytes.NewReader([]byte("Content of file 1")),
		"file2.txt":     bytes.NewReader([]byte("Content of file 2")),
		"dir/file3.txt": bytes.NewReader([]byte("Content of file 3 in dir")),
	}

	// 测试批量上传
	err = storage.BatchUpload(context.Background(), files)
	if err != nil {
		t.Fatalf("BatchUpload failed: %v", err)
	}

	// 验证文件是否都已创建
	for filePath := range files {
		fullPath := filepath.Join(tempDir, filePath)
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			t.Fatalf("File %s was not created", filePath)
		}
	}
}

func TestLocalStorage_BatchDelete(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建本地存储实例
	storage := NewLocalStorage(LocalStorageConfig{
		BasePath: tempDir,
	})

	// 创建测试文件
	filePaths := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, filePath := range filePaths {
		fullPath := filepath.Join(tempDir, filePath)
		err = os.WriteFile(fullPath, []byte("test content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create test file %s: %v", filePath, err)
		}
	}

	// 确保文件都存在
	for _, filePath := range filePaths {
		fullPath := filepath.Join(tempDir, filePath)
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			t.Fatalf("Test file %s was not created", filePath)
		}
	}

	// 测试批量删除
	err = storage.BatchDelete(context.Background(), filePaths)
	if err != nil {
		t.Fatalf("BatchDelete failed: %v", err)
	}

	// 验证文件是否都被删除
	for _, filePath := range filePaths {
		fullPath := filepath.Join(tempDir, filePath)
		if _, err := os.Stat(fullPath); !os.IsNotExist(err) {
			t.Fatalf("File %s was not deleted", filePath)
		}
	}
}
