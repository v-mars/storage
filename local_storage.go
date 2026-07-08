package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/cloudwego/hertz/pkg/common/hlog"
)

// LocalStorageConfig 本地存储配置
type LocalStorageConfig struct {
	BasePath string `json:"base_path"` // 本地存储基础路径
}

// LocalStorage 本地存储实现
type LocalStorage struct {
	config LocalStorageConfig
}

// NewLocalStorage 创建新的本地存储实例
func NewLocalStorage(config LocalStorageConfig) Storage {
	return &LocalStorage{
		config: config,
	}
}

// Upload 实现本地文件上传（本地存储不支持有效期）
func (s *LocalStorage) Upload(ctx context.Context, filePath string, reader io.Reader, opts ...UploadOption) error {
	hlog.CtxInfof(ctx, "开始上传文件到本地存储: %s", filePath)

	fullPath := filepath.Join(s.config.BasePath, filePath)
	dir := filepath.Dir(fullPath)

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		hlog.CtxErrorf(ctx, "创建目录失败: %v", err)
		return err
	}

	file, err := os.Create(fullPath)
	if err != nil {
		hlog.CtxErrorf(ctx, "创建文件失败: %v", err)
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, reader)
	if err != nil {
		hlog.CtxErrorf(ctx, "写入文件失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "文件上传成功: %s", filePath)
	return nil
}

// Download 实现本地文件下载（流式下载）
func (s *LocalStorage) Download(ctx context.Context, filePath string) (io.Reader, error) {
	hlog.CtxInfof(ctx, "开始下载本地文件: %s", filePath)

	fullPath := filepath.Join(s.config.BasePath, filePath)

	// 创建管道：一端读取文件内容，另一端提供给调用者
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		// 打开文件
		file, err := os.Open(fullPath)
		if err != nil {
			hlog.CtxErrorf(ctx, "打开本地文件失败: %v", err)
			pw.CloseWithError(err)
			return
		}
		defer file.Close()

		// 流式写入管道
		if _, err := io.Copy(pw, file); err != nil {
			hlog.CtxErrorf(ctx, "本地文件流式下载失败: %v", err)
			pw.CloseWithError(err)
			return
		}

		hlog.CtxInfof(ctx, "本地文件下载成功: %s", filePath)
	}()

	return pr, nil
}

// DownloadRange 实现本地文件断点续传下载
func (s *LocalStorage) DownloadRange(ctx context.Context, filePath string, offset, size int64) (io.Reader, error) {
	hlog.CtxInfof(ctx, "开始本地文件断点续传下载: %s, offset=%d, size=%d", filePath, offset, size)

	fullPath := filepath.Join(s.config.BasePath, filePath)

	// 创建管道：一端读取文件内容，另一端提供给调用者
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		// 打开文件
		file, err := os.Open(fullPath)
		if err != nil {
			hlog.CtxErrorf(ctx, "打开本地文件失败: %v", err)
			pw.CloseWithError(err)
			return
		}
		defer file.Close()

		// 移动到指定偏移量
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			hlog.CtxErrorf(ctx, "设置文件偏移量失败: %v", err)
			pw.CloseWithError(err)
			return
		}

		// 限制读取大小
		reader := io.LimitReader(file, size)

		// 流式写入管道
		if _, err := io.Copy(pw, reader); err != nil {
			hlog.CtxErrorf(ctx, "本地文件流式下载失败: %v", err)
			pw.CloseWithError(err)
			return
		}

		hlog.CtxInfof(ctx, "本地文件断点续传下载成功: %s", filePath)
	}()

	return pr, nil
}

// Delete 实现删除本地文件
func (s *LocalStorage) Delete(ctx context.Context, filePath string) error {
	hlog.CtxInfof(ctx, "开始删除本地文件: %s", filePath)

	fullPath := filepath.Join(s.config.BasePath, filePath)

	err := os.Remove(fullPath)
	if err != nil {
		hlog.CtxErrorf(ctx, "删除文件失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "文件删除成功: %s", filePath)
	return nil
}

// Rename 实现本地文件重命名
func (s *LocalStorage) Rename(ctx context.Context, oldPath string, newPath string) error {
	hlog.CtxInfof(ctx, "开始重命名本地文件: %s -> %s", oldPath, newPath)

	oldFullPath := filepath.Join(s.config.BasePath, oldPath)
	newFullPath := filepath.Join(s.config.BasePath, newPath)

	// 确保目标目录存在
	if err := os.MkdirAll(filepath.Dir(newFullPath), os.ModePerm); err != nil {
		hlog.CtxErrorf(ctx, "创建目标目录失败: %v", err)
		return err
	}

	err := os.Rename(oldFullPath, newFullPath)
	if err != nil {
		hlog.CtxErrorf(ctx, "文件重命名失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "文件重命名成功: %s -> %s", oldPath, newPath)
	return nil
}

// Move 实现本地文件移动
func (s *LocalStorage) Move(ctx context.Context, srcPath string, dstPath string) error {
	hlog.CtxInfof(ctx, "开始移动本地文件: %s -> %s", srcPath, dstPath)
	return s.Rename(ctx, srcPath, dstPath)
}

// Copy 实现本地文件复制
func (s *LocalStorage) Copy(ctx context.Context, srcPath string, dstPath string) error {
	hlog.CtxInfof(ctx, "开始复制本地文件: %s -> %s", srcPath, dstPath)

	srcFullPath := filepath.Join(s.config.BasePath, srcPath)
	dstFullPath := filepath.Join(s.config.BasePath, dstPath)

	// 确保目标目录存在
	if err := os.MkdirAll(filepath.Dir(dstFullPath), os.ModePerm); err != nil {
		hlog.CtxErrorf(ctx, "创建目标目录失败: %v", err)
		return err
	}

	srcFile, err := os.Open(srcFullPath)
	if err != nil {
		hlog.CtxErrorf(ctx, "打开源文件失败: %v", err)
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dstFullPath)
	if err != nil {
		hlog.CtxErrorf(ctx, "创建目标文件失败: %v", err)
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		hlog.CtxErrorf(ctx, "复制文件内容失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "文件复制成功: %s -> %s", srcPath, dstPath)
	return nil
}

// Exists 实现检查本地文件是否存在
func (s *LocalStorage) Exists(ctx context.Context, filePath string) (bool, error) {
	fullPath := filepath.Join(s.config.BasePath, filePath)
	_, err := os.Stat(fullPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// CreateDir 实现本地目录创建
func (s *LocalStorage) CreateDir(ctx context.Context, dirPath string) error {
	hlog.CtxInfof(ctx, "开始创建本地目录: %s", dirPath)

	fullPath := filepath.Join(s.config.BasePath, dirPath)

	err := os.MkdirAll(fullPath, os.ModePerm)
	if err != nil {
		hlog.CtxErrorf(ctx, "创建目录失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "目录创建成功: %s", dirPath)
	return nil
}

// DeleteDir 实现本地目录删除
func (s *LocalStorage) DeleteDir(ctx context.Context, dirPath string) error {
	hlog.CtxInfof(ctx, "开始删除本地目录: %s", dirPath)

	fullPath := filepath.Join(s.config.BasePath, dirPath)

	err := os.RemoveAll(fullPath)
	if err != nil {
		hlog.CtxErrorf(ctx, "删除目录失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "目录删除成功: %s", dirPath)
	return nil
}

// ListDir 实现本地目录列表（仅列出当前层级，不递归）
func (s *LocalStorage) ListDir(ctx context.Context, dirPath string) ([]FileMetadata, error) {
	hlog.CtxInfof(ctx, "开始列出本地目录内容: %s", dirPath)

	fullPath := filepath.Join(s.config.BasePath, dirPath)

	entries, err := os.ReadDir(fullPath)
	if err != nil {
		hlog.CtxErrorf(ctx, "列出目录内容失败: %v", err)
		return nil, err
	}

	files := make([]FileMetadata, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		metadata := FileMetadata{
			Name:     entry.Name(),
			Size:     info.Size(),
			ModTime:  info.ModTime(),
			IsDir:    entry.IsDir(),
			MIMEType: "application/octet-stream",
		}
		files = append(files, metadata)
	}

	hlog.CtxInfof(ctx, "成功列出目录内容: %s, 共找到 %d 个条目", dirPath, len(files))
	return files, nil
}

// GetMetadata 获取本地文件元数据
func (s *LocalStorage) GetMetadata(ctx context.Context, filePath string) (*FileMetadata, error) {
	hlog.CtxInfof(ctx, "开始获取本地文件元数据: %s", filePath)

	fullPath := filepath.Join(s.config.BasePath, filePath)

	info, err := os.Stat(fullPath)
	if err != nil {
		hlog.CtxErrorf(ctx, "获取文件信息失败: %v", err)
		return nil, err
	}

	metadata := &FileMetadata{
		Name:     filePath,
		Size:     info.Size(),
		ModTime:  info.ModTime(),
		IsDir:    info.IsDir(),
		MIMEType: "application/octet-stream", // 简化处理，实际应根据文件类型判断
	}

	hlog.CtxInfof(ctx, "成功获取文件元数据: %s", filePath)
	return metadata, nil
}

// UpdateMetadata 更新本地文件元数据
func (s *LocalStorage) UpdateMetadata(ctx context.Context, filePath string, metadata *FileMetadata) error {
	hlog.CtxInfof(ctx, "开始更新本地文件元数据: %s", filePath)

	// 本地文件系统中只能更新部分可修改的元数据（如修改时间）
	fullPath := filepath.Join(s.config.BasePath, filePath)

	if !metadata.ModTime.IsZero() {
		err := os.Chtimes(fullPath, metadata.ModTime, metadata.ModTime)
		if err != nil {
			hlog.CtxErrorf(ctx, "更新文件时间失败: %v", err)
			return err
		}
	}

	hlog.CtxInfof(ctx, "成功更新文件元数据: %s", filePath)
	return nil
}

// BatchUpload 实现批量上传
func (s *LocalStorage) BatchUpload(ctx context.Context, files map[string]io.Reader, opts ...UploadOption) error {
	hlog.CtxInfof(ctx, "开始批量上传 %d 个文件", len(files))
	return BatchUploadHelper(ctx, s, files, opts...)
}

// BatchDownload 实现本地批量下载（流式下载）
func (s *LocalStorage) BatchDownload(ctx context.Context, filePaths []string) (map[string]io.Reader, error) {
	hlog.CtxInfof(ctx, "开始批量下载 %d 个本地文件", len(filePaths))
	return BatchDownloadHelper(ctx, s, filePaths)
}

// BatchDelete 实现批量删除
func (s *LocalStorage) BatchDelete(ctx context.Context, filePaths []string) error {
	hlog.CtxInfof(ctx, "开始批量删除 %d 个文件", len(filePaths))
	return BatchDeleteHelper(ctx, s, filePaths)
}
