package storage

import (
	"context"
	"io"
	"time"
)

// StorageType 定义支持的存储类型
type StorageType string

const (
	Local StorageType = "local" // 本地存储类型
	OSS   StorageType = "oss"   // 阿里云OSS存储类型
	MinIO StorageType = "minio" // MinIO存储类型
	S3    StorageType = "s3"    // 标准S3存储类型
)

// FileMetadata 文件元数据
type FileMetadata struct {
	Name     string    `json:"name"`      // 文件名
	Size     int64     `json:"size"`      // 文件大小
	ModTime  time.Time `json:"mod_time"`  // 修改时间
	IsDir    bool      `json:"is_dir"`    // 是否为目录
	MIMEType string    `json:"mime_type"` // MIME 类型
}

// Storage 接口定义了统一的存储操作
type Storage interface {
	// 基础操作
	Upload(ctx context.Context, filePath string, reader io.Reader, opts ...UploadOption) error
	Download(ctx context.Context, filePath string) (io.Reader, error)                          // 修改为返回io.Reader
	DownloadRange(ctx context.Context, filePath string, offset, size int64) (io.Reader, error) // 新增断点续传下载
	Delete(ctx context.Context, filePath string) error
	Rename(ctx context.Context, oldPath string, newPath string) error
	Move(ctx context.Context, srcPath string, dstPath string) error
	Copy(ctx context.Context, srcPath string, dstPath string) error
	Exists(ctx context.Context, filePath string) (bool, error) // 检查文件是否存在

	// 目录操作
	CreateDir(ctx context.Context, dirPath string) error
	DeleteDir(ctx context.Context, dirPath string) error
	ListDir(ctx context.Context, dirPath string) ([]FileMetadata, error)

	// 元数据管理
	GetMetadata(ctx context.Context, filePath string) (*FileMetadata, error)
	UpdateMetadata(ctx context.Context, filePath string, metadata *FileMetadata) error

	// 批量操作
	BatchUpload(ctx context.Context, files map[string]io.Reader, opts ...UploadOption) error
	BatchDownload(ctx context.Context, filePaths []string) (map[string]io.Reader, error) // 修改为返回io.Reader映射
	BatchDelete(ctx context.Context, filePaths []string) error
}
