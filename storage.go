package storage

import (
	"context"
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// StorageType 定义支持的存储类型
type StorageType string

const (
	Local StorageType = "local" // 添加本地存储类型
	OSS   StorageType = "oss"   // 添加阿里云OSS存储类型
	MinIO StorageType = "minio" // 添加MinIO存储类型
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
	Upload(ctx context.Context, filePath string, reader io.Reader) error
	Download(ctx context.Context, filePath string) (io.Reader, error)                          // 修改为返回io.Reader
	DownloadRange(ctx context.Context, filePath string, offset, size int64) (io.Reader, error) // 新增断点续传下载
	Delete(ctx context.Context, filePath string) error
	Rename(ctx context.Context, oldPath string, newPath string) error
	Move(ctx context.Context, srcPath string, dstPath string) error
	Copy(ctx context.Context, srcPath string, dstPath string) error

	// 目录操作
	CreateDir(ctx context.Context, dirPath string) error
	DeleteDir(ctx context.Context, dirPath string) error
	ListDir(ctx context.Context, dirPath string) ([]FileMetadata, error)

	// 元数据管理
	GetMetadata(ctx context.Context, filePath string) (*FileMetadata, error)
	UpdateMetadata(ctx context.Context, filePath string, metadata *FileMetadata) error

	// 批量操作
	BatchUpload(ctx context.Context, files map[string]io.Reader) error
	BatchDownload(ctx context.Context, filePaths []string) (map[string]io.Reader, error) // 修改为返回io.Reader映射
	BatchDelete(ctx context.Context, filePaths []string) error
}

type Types struct {
	Mode       StorageType        `yaml:"mode" json:"mode"`               // local, s3, minio, oss, cos,
	AssignMode StorageType        `yaml:"assign_mode" json:"assign_mode"` // local, s3, minio, oss, cos,
	MaxSize    int64              `yaml:"max_size" json:"max_size"`
	Local      LocalStorageConfig `json:"local"`
	Minio      MinIOStorageConfig `json:"minio"`
	Oss        OSSStorageConfig   `json:"oss"`
}

// StorageOption 定义存储选项函数类型
type StorageOption func(*Types)

// WithLocalConfig 设置本地存储配置选项
func WithLocalConfig(config LocalStorageConfig) StorageOption {
	return func(s *Types) {
		s.Local = config
		s.Mode = Local
	}
}

// WithMinIOConfig 设置MinIO存储配置选项
func WithMinIOConfig(config MinIOStorageConfig) StorageOption {
	return func(s *Types) {
		s.Minio = config
		s.Mode = MinIO
	}
}

// WithOSSConfig 设置OSS存储配置选项
func WithOSSConfig(config OSSStorageConfig) StorageOption {
	return func(s *Types) {
		s.Oss = config
		s.Mode = OSS
	}
}

// WithMode 设置存储模式选项
func WithMode(mode StorageType) StorageOption {
	return func(s *Types) {
		s.Mode = mode
	}
}

// WithAssignMode 设置指定存储模式选项
func WithAssignMode(mode StorageType) StorageOption {
	return func(s *Types) {
		s.AssignMode = mode
	}
}

// WithMaxSize 设置最大文件大小选项
func WithMaxSize(maxSize int64) StorageOption {
	return func(s *Types) {
		s.MaxSize = maxSize
	}
}

// DefaultStorageOptions 默认存储选项
func DefaultStorageOptions() []StorageOption {
	return []StorageOption{
		WithMode(Local),
		WithLocalConfig(LocalStorageConfig{
			BasePath: os.TempDir(), // 默认使用系统临时目录
		}),
	}
}

// GetStorage 使用选项模式获取存储实例
func (s *Types) GetStorage(ctx context.Context, opts ...StorageOption) (string, Storage) {
	// 应用所有选项
	for _, opt := range opts {
		opt(s)
	}

	if s.AssignMode == "" && s.Mode != "" {
		// 如果没有指定分配模式，则使用默认模式
		s.AssignMode = s.Mode
	} else if s.AssignMode == "" && s.Mode == "" {
		// 使用默认配置
		opts = DefaultStorageOptions()
	}

	// 根据模式返回相应的存储实例
	switch s.AssignMode {
	case MinIO:
		// 验证MinIO配置
		if s.Minio.BaseDir == "" || s.Minio.Endpoint == "" || s.Minio.AccessKeyID == "" || s.Minio.AccessKeySecret == "" || s.Minio.Bucket == "" {
			hlog.CtxErrorf(ctx, "MinIO config error: missing required fields")
			return "", nil
		}
		hlog.CtxInfof(ctx, "Using MinIO storage")
		return s.Minio.BaseDir, NewMinIOStorage(s.Minio)
	case OSS:
		// 验证OSS配置
		if s.Oss.BaseDir == "" || s.Oss.Endpoint == "" || s.Oss.AccessKeyID == "" || s.Oss.AccessKeySecret == "" || s.Oss.Bucket == "" {
			hlog.CtxErrorf(ctx, "OSS config error: missing required fields")
			return "", nil
		}
		hlog.CtxInfof(ctx, "Using OSS storage")
		return s.Oss.BaseDir, NewOSSStorage(s.Oss)
	default:
		// 默认使用本地存储
		if s.Local.BasePath == "" {
			hlog.CtxErrorf(ctx, "Local storage base path is empty")
			return "", nil
		}
		hlog.CtxInfof(ctx, "Using Local storage")
		return s.Local.BasePath, NewLocalStorage(s.Local)
	}
}

//################## 存储工厂 #####################

var storageDrivers = make(map[StorageType]func() Storage)

// RegisterStorageDriver 注册存储驱动
func RegisterStorageDriver(storageType StorageType, factory func() Storage) {
	storageDrivers[storageType] = factory
}

// CreateStorage 创建存储实例
func CreateStorage(storageType StorageType) Storage {
	if factory, ok := storageDrivers[storageType]; ok {
		return factory()
	}
	hlog.Errorf("不支持的存储类型: %v", storageType)
	return nil
}

//################## 初始化函数 #####################

func init() {
	// 注册本地存储驱动
	//RegisterStorageDriver(Local, func() Storage {
	//	// 这里可以添加默认配置或从配置中心获取
	//	return NewLocalStorage(LocalStorageConfig{
	//		BasePath: os.TempDir(), // 默认使用系统临时目录
	//	})
	//})

	// 注册OSS存储驱动（示例配置）
	//RegisterStorageDriver(OSS, func() Storage {
	//	// 实际使用时应从安全的配置源获取
	//	return NewOSSStorage(OSSStorageConfig{
	//		Endpoint:        "your-endpoint",
	//		AccessKeyID:     "your-access-key-id",
	//		AccessKeySecret: "your-access-key-secret",
	//		Bucket:          "your-bucket-name",
	//		BaseDir:         "your-base-dir",
	//	})
	//})

	// 注册MinIO存储驱动（示例配置）
	//RegisterStorageDriver(MinIO, func() Storage {
	//	// 实际使用时应从安全的配置源获取
	//	return NewMinIOStorage(MinIOStorageConfig{
	//		Endpoint:        "your-minio-endpoint",
	//		AccessKeyID:     "your-minio-access-key-id",
	//		AccessKeySecret: "your-minio-access-key-secret",
	//		UseSSL:          false,
	//		Bucket:          "your-minio-bucket",
	//		BaseDir:         "your-minio-base-dir",
	//	})
	//})
}

//################## 辅助函数 #####################

// 确保OSS目录路径以/结尾
func ensureOSSDirPath(path string) string {
	if len(path) > 0 && path[len(path)-1] != '/' {
		path += "/"
	}
	return path
}

// 检查是否为目录占位符
func isDirectoryPlaceholder(key, dirPath string) bool {
	// 目录占位符通常只有一个斜杠结尾
	return key == dirPath || key == dirPath+"/"
}

// ChunkedReader 封装reader以实现分块读取
type ChunkedReader struct {
	reader    io.Reader
	chunkSize int
}

// NewChunkedReader 创建新的分块读取器
func NewChunkedReader(reader io.Reader, chunkSize int) *ChunkedReader {
	return &ChunkedReader{
		reader:    reader,
		chunkSize: chunkSize,
	}
}

// Read 实现分块读取
func (r *ChunkedReader) Read(p []byte) (n int, err error) {
	// 限制每次读取的数据量
	limit := len(p)
	if limit > r.chunkSize {
		limit = r.chunkSize
	}

	return r.reader.Read(p[:limit])
}

//################## 本地存储配置 #####################

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

// Upload 实现本地文件上传
func (s *LocalStorage) Upload(ctx context.Context, filePath string, reader io.Reader) error {
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

func (s *LocalStorage) DownloadBak(ctx context.Context, filePath string) ([]byte, error) {
	hlog.CtxInfof(ctx, "开始下载本地文件: %s", filePath)

	fullPath := filepath.Join(s.config.BasePath, filePath)

	// 打开文件
	file, err := os.Open(fullPath)
	if err != nil {
		hlog.CtxErrorf(ctx, "打开本地文件失败: %v", err)
		return nil, err
	}
	defer file.Close()

	// 创建临时文件用于缓冲下载内容
	tmpFile, err := os.CreateTemp("", "local-download-*")
	if err != nil {
		hlog.CtxErrorf(ctx, "创建临时文件失败: %v", err)
		return nil, err
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name()) // 清理临时文件

	// 流式下载：逐块写入临时文件
	if _, err := io.Copy(tmpFile, file); err != nil {
		hlog.CtxErrorf(ctx, "本地文件流式下载失败: %v", err)
		return nil, err
	}

	// 从头开始读取临时文件内容
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		hlog.CtxErrorf(ctx, "重置文件指针失败: %v", err)
		return nil, err
	}

	// 读取文件内容到内存
	content, err := io.ReadAll(tmpFile)
	if err != nil {
		hlog.CtxErrorf(ctx, "读取本地文件内容失败: %v", err)
		return nil, err
	}

	hlog.CtxInfof(ctx, "本地文件下载成功: %s", filePath)
	return content, nil
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

// ListDir 实现本地目录列表
func (s *LocalStorage) ListDir(ctx context.Context, dirPath string) ([]FileMetadata, error) {
	hlog.CtxInfof(ctx, "开始列出本地目录内容: %s", dirPath)

	fullPath := filepath.Join(s.config.BasePath, dirPath)

	files := make([]FileMetadata, 0)

	err := filepath.Walk(fullPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, _ := filepath.Rel(s.config.BasePath, path)
		metadata := FileMetadata{
			Name:     relPath,
			Size:     info.Size(),
			ModTime:  info.ModTime(),
			IsDir:    info.IsDir(),
			MIMEType: "application/octet-stream", // 简化处理，实际应根据文件类型判断
		}
		files = append(files, metadata)
		return nil
	})

	if err != nil {
		hlog.CtxErrorf(ctx, "列出目录内容失败: %v", err)
		return nil, err
	}

	hlog.CtxInfof(ctx, "成功列出目录内容: %s, 共找到 %d 个文件", dirPath, len(files))
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

	if metadata.ModTime.IsZero() {
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
func (s *LocalStorage) BatchUpload(ctx context.Context, files map[string]io.Reader) error {
	hlog.CtxInfof(ctx, "开始批量上传 %d 个文件", len(files))

	for filePath, reader := range files {
		if err := s.Upload(ctx, filePath, reader); err != nil {
			hlog.CtxErrorf(ctx, "批量上传失败，文件: %s, 错误: %v", filePath, err)
			return err
		}
	}

	hlog.CtxInfof(ctx, "成功完成批量上传，共 %d 个文件", len(files))
	return nil
}

// BatchDownload 实现本地批量下载（流式下载）
func (s *LocalStorage) BatchDownload(ctx context.Context, filePaths []string) (map[string]io.Reader, error) {
	hlog.CtxInfof(ctx, "开始批量下载 %d 个本地文件", len(filePaths))

	results := make(map[string]io.Reader)

	for _, filePath := range filePaths {
		reader, err := s.Download(ctx, filePath)
		if err != nil {
			hlog.CtxErrorf(ctx, "本地批量下载失败，文件: %s, 错误: %v", filePath, err)
			// 关闭已打开的reader
			for _, r := range results {
				if closer, ok := r.(io.Closer); ok {
					closer.Close()
				}
			}
			return nil, err
		}
		results[filePath] = reader
	}

	hlog.CtxInfof(ctx, "成功完成本地批量下载，共 %d 个文件", len(filePaths))
	return results, nil
}

// BatchDelete 实现批量删除
func (s *LocalStorage) BatchDelete(ctx context.Context, filePaths []string) error {
	hlog.CtxInfof(ctx, "开始批量删除 %d 个文件", len(filePaths))

	for _, filePath := range filePaths {
		if err := s.Delete(ctx, filePath); err != nil {
			hlog.CtxErrorf(ctx, "批量删除失败，文件: %s, 错误: %v", filePath, err)
			return err
		}
	}

	hlog.CtxInfof(ctx, "成功完成批量删除，共 %d 个文件", len(filePaths))
	return nil
}

//################## OSS 存储 #####################

// OSSStorageConfig OSS 存储配置
type OSSStorageConfig struct {
	Endpoint        string `json:"endpoint"`          // OSS endpoint
	AccessKeyID     string `json:"access_key_id"`     // Access Key ID
	AccessKeySecret string `json:"access_key_secret"` // Access Key Secret
	Bucket          string `json:"bucket"`            // 存储桶名称
	BaseDir         string `json:"base_dir"`          // 存储基础目录
}

// OSSStorage OSS 存储实现
type OSSStorage struct {
	config OSSStorageConfig
	client *oss.Client
	bucket *oss.Bucket
}

// NewOSSStorage 创建新的OSS存储实例
func NewOSSStorage(config OSSStorageConfig) Storage {
	client, err := oss.New(config.Endpoint, config.AccessKeyID, config.AccessKeySecret)
	if err != nil {
		hlog.Errorf("创建OSS客户端失败: %v", err)
		return nil
	}

	bucket, err := client.Bucket(config.Bucket)
	if err != nil {
		hlog.Errorf("获取Bucket失败: %v", err)
		return nil
	}

	return &OSSStorage{
		config: config,
		client: client,
		bucket: bucket,
	}
}

// Upload 实现OSS文件上传
func (s *OSSStorage) Upload(ctx context.Context, filePath string, reader io.Reader) error {
	hlog.CtxInfof(ctx, "开始上传文件到OSS: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	err := s.bucket.PutObject(fullKey, reader)
	if err != nil {
		hlog.CtxErrorf(ctx, "OSS上传文件失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "OSS文件上传成功: %s", filePath)
	return nil
}

// Download 实现OSS文件下载（流式下载）
func (s *OSSStorage) Download(ctx context.Context, filePath string) (io.Reader, error) {
	hlog.CtxInfof(ctx, "开始从OSS下载文件: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	body, err := s.bucket.GetObject(fullKey)
	if err != nil {
		hlog.CtxErrorf(ctx, "OSS获取文件失败: %v", err)
		return nil, err
	}

	hlog.CtxInfof(ctx, "OSS文件下载已启动: %s", filePath)
	return body, nil // 返回原始的Reader，由调用方负责关闭
}

// DownloadRange 实现OSS文件断点续传下载
func (s *OSSStorage) DownloadRange(ctx context.Context, filePath string, offset, size int64) (io.Reader, error) {
	hlog.CtxInfof(ctx, "开始OSS文件断点续传下载: %s, offset=%d, size=%d", filePath, offset, size)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	// 构建范围请求
	//rangeStr := fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)
	body, err := s.bucket.GetObject(fullKey, oss.Range(offset, offset+size-1))
	if err != nil {
		hlog.CtxErrorf(ctx, "OSS获取文件范围失败: %v", err)
		return nil, err
	}

	hlog.CtxInfof(ctx, "OSS文件断点续传下载已启动: %s", filePath)
	return body, nil // 返回原始的Reader，由调用方负责关闭
}

func (s *OSSStorage) DownloadBak(ctx context.Context, filePath string) ([]byte, error) {
	hlog.CtxInfof(ctx, "开始从OSS下载文件: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	body, err := s.bucket.GetObject(fullKey)
	if err != nil {
		hlog.CtxErrorf(ctx, "OSS获取文件失败: %v", err)
		return nil, err
	}
	defer body.Close()

	// 创建临时文件用于缓冲下载内容
	tmpFile, err := os.CreateTemp("", "oss-download-*")
	if err != nil {
		hlog.CtxErrorf(ctx, "创建临时文件失败: %v", err)
		return nil, err
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name()) // 清理临时文件

	// 流式下载：逐块写入临时文件
	if _, err := io.Copy(tmpFile, body); err != nil {
		hlog.CtxErrorf(ctx, "OSS流式下载失败: %v", err)
		return nil, err
	}

	// 从头开始读取临时文件内容
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		hlog.CtxErrorf(ctx, "重置文件指针失败: %v", err)
		return nil, err
	}

	// 读取文件内容到内存
	content, err := io.ReadAll(tmpFile)
	if err != nil {
		hlog.CtxErrorf(ctx, "读取OSS文件内容失败: %v", err)
		return nil, err
	}

	hlog.CtxInfof(ctx, "OSS文件下载成功: %s", filePath)
	return content, nil
}

// Delete 实现OSS文件删除
func (s *OSSStorage) Delete(ctx context.Context, filePath string) error {
	hlog.CtxInfof(ctx, "开始从OSS删除文件: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	err := s.bucket.DeleteObject(fullKey)
	if err != nil {
		hlog.CtxErrorf(ctx, "OSS删除文件失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "OSS文件删除成功: %s", filePath)
	return nil
}

// Rename 实现OSS文件重命名（复制+删除）
func (s *OSSStorage) Rename(ctx context.Context, oldPath string, newPath string) error {
	hlog.CtxInfof(ctx, "开始在OSS中重命名文件: %s -> %s", oldPath, newPath)

	oldFullKey := filepath.Join(s.config.BaseDir, oldPath)
	newFullKey := filepath.Join(s.config.BaseDir, newPath)

	// 复制文件到新路径
	_, err := s.bucket.CopyObject(oldFullKey, newFullKey)
	if err != nil {
		hlog.CtxErrorf(ctx, "OSS复制文件失败: %v", err)
		return err
	}

	// 删除旧文件
	if err = s.Delete(ctx, oldPath); err != nil {
		hlog.CtxErrorf(ctx, "OSS删除旧文件失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "OSS文件重命名成功: %s -> %s", oldPath, newPath)
	return nil
}

// Move 实现OSS文件移动（与重命名相同的操作）
func (s *OSSStorage) Move(ctx context.Context, srcPath string, dstPath string) error {
	return s.Rename(ctx, srcPath, dstPath)
}

// Copy 实现OSS文件复制
func (s *OSSStorage) Copy(ctx context.Context, srcPath string, dstPath string) error {
	hlog.CtxInfof(ctx, "开始在OSS中复制文件: %s -> %s", srcPath, dstPath)

	oldFullKey := filepath.Join(s.config.BaseDir, srcPath)
	newFullKey := filepath.Join(s.config.BaseDir, dstPath)

	_, err := s.bucket.CopyObject(oldFullKey, newFullKey)
	if err != nil {
		hlog.CtxErrorf(ctx, "OSS复制文件失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "OSS文件复制成功: %s -> %s", srcPath, dstPath)
	return nil
}

// CreateDir 在OSS中创建目录（通过创建以/结尾的对象模拟目录）
func (s *OSSStorage) CreateDir(ctx context.Context, dirPath string) error {
	hlog.CtxInfof(ctx, "开始在OSS中创建目录: %s", dirPath)

	dirPath = ensureOSSDirPath(dirPath)
	fullKey := filepath.Join(s.config.BaseDir, dirPath)

	// 检查目录是否已存在
	exist, err := s.bucket.IsObjectExist(fullKey)
	if err != nil {
		hlog.CtxErrorf(ctx, "检查OSS目录是否存在失败: %v", err)
		return err
	}

	if exist {
		hlog.CtxDebugf(ctx, "OSS目录已存在，无需创建: %s", fullKey)
		return nil
	}

	// 创建空对象作为目录占位符
	if err = s.bucket.PutObject(fullKey, strings.NewReader("")); err != nil {
		hlog.CtxErrorf(ctx, "创建OSS目录占位符失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "OSS目录创建成功: %s", fullKey)
	return nil
}

// DeleteDir 从OSS中删除目录（递归删除目录下所有对象）
func (s *OSSStorage) DeleteDir(ctx context.Context, dirPath string) error {
	hlog.CtxInfof(ctx, "开始从OSS中删除目录及其所有内容: %s", dirPath)

	dirPath = ensureOSSDirPath(dirPath)
	fullKey := filepath.Join(s.config.BaseDir, dirPath)

	// 获取目录下的所有对象
	marker := ""

	for {
		// 分别处理Prefix和Marker
		var listOptions []oss.Option

		// 添加Prefix选项
		listOptions = append(listOptions, oss.Prefix(fullKey))

		// 如果有marker，添加Marker选项
		if marker != "" {
			listOptions = append(listOptions, oss.Marker(marker))
		}

		// 执行列表操作 - 注意：OSS SDK实际上只返回两个参数
		objectListing, err := s.bucket.ListObjects(listOptions...)
		if err != nil {
			hlog.CtxErrorf(ctx, "列出OSS目录内容失败: %v", err)
			return fmt.Errorf("列出OSS目录内容失败：%v", err)
		}

		// 删除目录下的所有对象
		for _, object := range objectListing.Objects {
			// 增加更严格的对象键检查
			if strings.HasPrefix(object.Key, fullKey) && !isDirectoryPlaceholder(object.Key, fullKey) {
				if err = s.Delete(ctx, object.Key); err != nil {
					hlog.CtxErrorf(ctx, "删除OSS对象失败: %v", err)
					return fmt.Errorf("删除OSS对象失败：%v", err)
				}
			}
		}

		// 如果当前批次返回的对象数量少于最大限制，则认为是最后一批
		if len(objectListing.Objects) < 1000 { // OSS默认最多每次返回1000个对象
			break
		}

		// 使用最后一个对象的Key作为下次查询的marker
		marker = objectListing.NextMarker
	}

	hlog.CtxInfof(ctx, "成功从OSS中删除目录及其所有内容: %s", fullKey)
	return nil
}

// ListDir 列出OSS目录内容
func (s *OSSStorage) ListDir(ctx context.Context, dirPath string) ([]FileMetadata, error) {
	hlog.CtxInfof(ctx, "开始列出OSS目录内容: %s", dirPath)

	dirPath = ensureOSSDirPath(dirPath)
	fullKey := filepath.Join(s.config.BaseDir, dirPath)

	var fileMetas []FileMetadata
	var marker string

	// 使用Prefix和Marker进行分页查询
	for {
		var listOptions []oss.Option

		// 添加Prefix选项
		listOptions = append(listOptions, oss.Prefix(fullKey))

		// 如果有marker，添加Marker选项
		if marker != "" {
			listOptions = append(listOptions, oss.Marker(marker))
		}

		// 执行列表操作
		objectListing, err := s.bucket.ListObjects(listOptions...)
		if err != nil {
			hlog.CtxErrorf(ctx, "获取OSS目录内容失败: %v", err)
			return nil, fmt.Errorf("获取OSS目录内容失败：%v", err)
		}

		// 转换对象信息为FileMeta
		for _, object := range objectListing.Objects {
			// 确保对象在目标目录下
			if strings.HasPrefix(object.Key, fullKey) && !isDirectoryPlaceholder(object.Key, fullKey) {
				fileMeta := FileMetadata{
					Name:     object.Key[len(fullKey):], // 去除目录前缀
					Size:     object.Size,
					ModTime:  object.LastModified,
					IsDir:    false,
					MIMEType: "application/octet-stream",
				}
				fileMetas = append(fileMetas, fileMeta)
			}
		}

		// 如果当前批次返回的对象数量少于最大限制，则认为是最后一批
		if len(objectListing.Objects) < 1000 { // OSS默认最多每次返回1000个对象
			break
		}

		// 使用最后一个对象的Key作为下次查询的marker
		marker = objectListing.NextMarker
	}

	hlog.CtxInfof(ctx, "成功列出OSS目录内容: %s", dirPath)
	return fileMetas, nil
}

// GetMetadata 获取OSS文件元数据
func (s *OSSStorage) GetMetadata(ctx context.Context, filePath string) (*FileMetadata, error) {
	hlog.CtxInfof(ctx, "开始获取OSS文件元数据: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	// 获取对象属性
	props, err := s.bucket.GetObjectDetailedMeta(fullKey)
	if err != nil {
		hlog.CtxErrorf(ctx, "获取OSS文件元数据失败: %v", err)
		return nil, fmt.Errorf("获取OSS文件元数据失败：%v", err)
	}

	// 从HTTPHeader中解析ContentLength
	var size int64 = 0
	if contentLengthStr := props.Get("Content-Length"); contentLengthStr != "" {
		var contentLengthInt int
		_, err := fmt.Sscan(contentLengthStr, &contentLengthInt)
		if err == nil {
			size = int64(contentLengthInt)
		}
	}

	// 解析最后修改时间 - 使用标准的RFC1123时间格式
	modTimeStr := props.Get("Last-Modified")
	modTime, err := time.Parse(time.RFC1123, modTimeStr)
	if err != nil {
		// 尝试其他可能的时间格式作为备选方案
		modTime, err = time.Parse("YYYY-MM-DD HH:MM:SS", modTimeStr)
		if err != nil {
			hlog.CtxErrorf(ctx, "解析OSS文件最后修改时间失败: %v", err)
			return nil, fmt.Errorf("解析OSS文件最后修改时间失败：%v", err)
		}
	}

	// 构建元数据对象
	fileMeta := &FileMetadata{
		Name:     filePath,
		Size:     size,
		ModTime:  modTime,
		IsDir:    false,
		MIMEType: props.Get("Content-Type"),
	}

	hlog.CtxInfof(ctx, "成功获取OSS文件元数据: %s", filePath)
	return fileMeta, nil
}

// UpdateMetadata 更新OSS文件元数据
func (s *OSSStorage) UpdateMetadata(ctx context.Context, filePath string, metadata *FileMetadata) error {
	hlog.CtxInfof(ctx, "开始更新OSS文件元数据: %s", filePath)

	// OSS不支持直接更新元数据，除非重新上传文件
	// 这里可以选择仅记录日志或抛出错误
	hlog.CtxErrorf(ctx, "OSS不支持直接更新元数据")
	return fmt.Errorf("OSS不支持直接更新元数据")
}

// BatchUpload 实现OSS批量上传
func (s *OSSStorage) BatchUpload(ctx context.Context, files map[string]io.Reader) error {
	hlog.CtxInfof(ctx, "开始批量上传 %d 个文件到OSS", len(files))

	for filePath, reader := range files {
		if err := s.Upload(ctx, filePath, reader); err != nil {
			hlog.CtxErrorf(ctx, "批量上传失败，文件: %s, 错误: %v", filePath, err)
			return err
		}
	}

	hlog.CtxInfof(ctx, "成功完成OSS批量上传，共 %d 个文件", len(files))
	return nil
}

// BatchDownload 实现OSS批量下载（流式下载）
func (s *OSSStorage) BatchDownload(ctx context.Context, filePaths []string) (map[string]io.Reader, error) {
	hlog.CtxInfof(ctx, "开始批量下载 %d 个OSS文件", len(filePaths))

	results := make(map[string]io.Reader)

	for _, filePath := range filePaths {
		reader, err := s.Download(ctx, filePath)
		if err != nil {
			hlog.CtxErrorf(ctx, "OSS批量下载失败，文件: %s, 错误: %v", filePath, err)
			// 关闭已打开的reader
			for _, r := range results {
				if closer, ok := r.(io.Closer); ok {
					closer.Close()
				}
			}
			return nil, err
		}
		results[filePath] = reader
	}

	hlog.CtxInfof(ctx, "成功完成OSS批量下载，共 %d 个文件", len(filePaths))
	return results, nil
}

// BatchDelete 实现OSS批量删除
func (s *OSSStorage) BatchDelete(ctx context.Context, filePaths []string) error {
	hlog.CtxInfof(ctx, "开始批量删除 %d 个OSS文件", len(filePaths))

	for _, filePath := range filePaths {
		if err := s.Delete(ctx, filePath); err != nil {
			hlog.CtxErrorf(ctx, "批量删除失败，文件: %s, 错误: %v", filePath, err)
			return err
		}
	}

	hlog.CtxInfof(ctx, "成功完成OSS批量删除，共 %d 个文件", len(filePaths))
	return nil
}

//################## MinIO 存储 #####################

// MinIOStorageConfig MinIO 存储配置
type MinIOStorageConfig struct {
	Endpoint        string `json:"endpoint"`          // MinIO endpoint
	AccessKeyID     string `json:"access_key_id"`     // Access Key ID
	AccessKeySecret string `json:"access_key_secret"` // Access Key Secret
	UseSSL          bool   `json:"use_ssl"`           // 是否使用SSL
	Bucket          string `json:"bucket"`            // 存储桶名称
	BaseDir         string `json:"base_dir"`          // 存储基础目录
}

// MinIOStorage MinIO 存储实现
type MinIOStorage struct {
	config MinIOStorageConfig
	client *minio.Client
}

// NewMinIOStorage 创建新的MinIO存储实例
func NewMinIOStorage(config MinIOStorageConfig) Storage {
	// 初始化MinIO客户端
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.AccessKeySecret, ""),
		Secure: config.UseSSL,
	})
	if err != nil {
		hlog.Errorf("创建MinIO客户端失败: %v", err)
		return nil
	}

	// 检查Bucket是否存在，如果不存在则创建
	exists, err := client.BucketExists(context.Background(), config.Bucket)
	if err != nil {
		hlog.Errorf("检查Bucket存在性失败: %v", err)
		return nil
	}

	if !exists {
		err = client.MakeBucket(context.Background(), config.Bucket, minio.MakeBucketOptions{})
		if err != nil {
			hlog.Errorf("创建Bucket失败: %v", err)
			return nil
		}
		hlog.Infof("成功创建新Bucket: %s", config.Bucket)
	}

	return &MinIOStorage{
		config: config,
		client: client,
	}
}

// Upload 实现MinIO文件上传
func (s *MinIOStorage) Upload(ctx context.Context, filePath string, reader io.Reader) error {
	hlog.CtxInfof(ctx, "开始上传文件到MinIO: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	// 使用流式上传
	_, err := s.client.PutObject(ctx, s.config.Bucket, fullKey, reader, -1, minio.PutObjectOptions{})
	if err != nil {
		hlog.CtxErrorf(ctx, "MinIO上传文件失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "MinIO文件上传成功: %s", filePath)
	return nil
}

// Download 实现从MinIO下载文件（流式下载）
func (s *MinIOStorage) Download(ctx context.Context, filePath string) (io.Reader, error) {
	hlog.CtxInfof(ctx, "开始从MinIO下载文件: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	object, err := s.client.GetObject(ctx, s.config.Bucket, fullKey, minio.GetObjectOptions{})
	if err != nil {
		hlog.CtxErrorf(ctx, "MinIO获取文件失败: %v", err)
		return nil, err
	}

	hlog.CtxInfof(ctx, "MinIO文件下载已启动: %s", filePath)
	return object, nil // 返回原始的Reader，由调用方负责关闭
}

// DownloadRange 实现从MinIO下载文件（支持断点续传）
func (s *MinIOStorage) DownloadRange(ctx context.Context, filePath string, offset int64, size int64) (io.Reader, error) {
	hlog.CtxInfof(ctx, "开始从MinIO下载文件: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)
	var opts = minio.GetObjectOptions{}
	if err := opts.SetRange(offset, offset+size-1); err != nil {
		return nil, err
	}
	// 获取对象信息以确定文件大小
	object, err := s.client.GetObject(ctx, s.config.Bucket, fullKey, opts)
	if err != nil {
		hlog.CtxErrorf(ctx, "MinIO获取文件失败: %v", err)
		return nil, err
	}

	hlog.CtxInfof(ctx, "MinIO文件断点续传下载已启动: %s", filePath)
	// 使用分块读取器包装原始reader
	return object, nil
}
func (s *MinIOStorage) DownloadBak(ctx context.Context, filePath string) ([]byte, error) {
	hlog.CtxInfof(ctx, "开始从MinIO下载文件: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	// 获取对象
	object, err := s.client.GetObject(ctx, s.config.Bucket, fullKey, minio.GetObjectOptions{})
	if err != nil {
		hlog.CtxErrorf(ctx, "MinIO获取文件失败: %v", err)
		return nil, err
	}
	defer object.Close()

	// 创建临时文件用于缓冲下载内容
	tmpFile, err := os.CreateTemp("", "minio-download-*")
	if err != nil {
		hlog.CtxErrorf(ctx, "创建临时文件失败: %v", err)
		return nil, err
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name()) // 清理临时文件

	// 流式下载：逐块写入临时文件
	if _, err := io.Copy(tmpFile, object); err != nil {
		hlog.CtxErrorf(ctx, "MinIO流式下载失败: %v", err)
		return nil, err
	}

	// 从头开始读取临时文件内容
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		hlog.CtxErrorf(ctx, "重置文件指针失败: %v", err)
		return nil, err
	}

	// 读取文件内容到内存
	content, err := io.ReadAll(tmpFile)
	if err != nil {
		hlog.CtxErrorf(ctx, "读取MinIO文件内容失败: %v", err)
		return nil, err
	}

	hlog.CtxInfof(ctx, "MinIO文件下载成功: %s", filePath)
	return content, nil
}

// Delete 实现MinIO文件删除
func (s *MinIOStorage) Delete(ctx context.Context, filePath string) error {
	hlog.CtxInfof(ctx, "开始从MinIO删除文件: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	// 删除文件
	err := s.client.RemoveObject(ctx, s.config.Bucket, fullKey, minio.RemoveObjectOptions{ForceDelete: true})
	if err != nil {
		hlog.CtxErrorf(ctx, "MinIO删除文件失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "MinIO文件删除成功: %s", filePath)
	return nil
}

// Rename 实现MinIO文件重命名（复制+删除）
func (s *MinIOStorage) Rename(ctx context.Context, oldPath string, newPath string) error {
	hlog.CtxInfof(ctx, "开始在MinIO中重命名文件: %s -> %s", oldPath, newPath)

	oldFullKey := filepath.Join(s.config.BaseDir, oldPath)
	newFullKey := filepath.Join(s.config.BaseDir, newPath)

	// 复制文件到新路径
	srcOpts := minio.CopySrcOptions{
		Bucket: s.config.Bucket,
		Object: oldFullKey,
	}
	dstOpts := minio.CopyDestOptions{
		Bucket: s.config.Bucket,
		Object: newFullKey,
	}

	_, err := s.client.CopyObject(ctx, dstOpts, srcOpts)
	if err != nil {
		hlog.CtxErrorf(ctx, "MinIO复制文件失败: %v", err)
		return err
	}

	// 删除旧文件
	if err = s.Delete(ctx, oldPath); err != nil {
		hlog.CtxErrorf(ctx, "MinIO删除旧文件失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "MinIO文件重命名成功: %s -> %s", oldPath, newPath)
	return nil
}

// Move 实现MinIO文件移动（与重命名相同的操作）
func (s *MinIOStorage) Move(ctx context.Context, srcPath string, dstPath string) error {
	hlog.CtxInfof(ctx, "开始在MinIO中移动文件: %s -> %s", srcPath, dstPath)
	return s.Rename(ctx, srcPath, dstPath)
}

// Copy 实现MinIO文件复制
func (s *MinIOStorage) Copy(ctx context.Context, srcPath string, dstPath string) error {
	hlog.CtxInfof(ctx, "开始在MinIO中复制文件: %s -> %s", srcPath, dstPath)

	srcFullKey := filepath.Join(s.config.BaseDir, srcPath)
	dstFullKey := filepath.Join(s.config.BaseDir, dstPath)

	// 复制文件
	srcOpts := minio.CopySrcOptions{
		Bucket: s.config.Bucket,
		Object: srcFullKey,
	}
	dstOpts := minio.CopyDestOptions{
		Bucket: s.config.Bucket,
		Object: dstFullKey,
	}

	_, err := s.client.CopyObject(ctx, dstOpts, srcOpts)
	if err != nil {
		hlog.CtxErrorf(ctx, "MinIO复制文件失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "MinIO文件复制成功: %s -> %s", srcPath, dstPath)
	return nil
}

// CreateDir 实现MinIO目录创建（通过创建以/结尾的对象模拟目录）
func (s *MinIOStorage) CreateDir(ctx context.Context, dirPath string) error {
	hlog.CtxInfof(ctx, "开始在MinIO中创建目录: %s", dirPath)

	dirPath = ensureOSSDirPath(dirPath)
	fullKey := filepath.Join(s.config.BaseDir, dirPath)

	// 检查目录是否已存在
	_, err := s.client.StatObject(ctx, s.config.Bucket, fullKey, minio.StatObjectOptions{})
	if err == nil {
		hlog.CtxDebugf(ctx, "MinIO目录已存在，无需创建: %s", fullKey)
		return nil
	}

	// 创建空对象作为目录占位符
	if err = s.Upload(ctx, dirPath, strings.NewReader("")); err != nil {
		hlog.CtxErrorf(ctx, "MinIO创建目录占位符失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "MinIO目录创建成功: %s", fullKey)
	return nil
}

// DeleteDir 实现MinIO目录删除（递归删除目录下所有对象）
func (s *MinIOStorage) DeleteDir(ctx context.Context, dirPath string) error {
	hlog.CtxInfof(ctx, "开始从MinIO中删除目录及其所有内容: %s", dirPath)

	dirPath = ensureOSSDirPath(dirPath)
	fullKey := filepath.Join(s.config.BaseDir, dirPath)

	// 列出目录下的所有对象
	for object := range s.client.ListObjects(ctx, s.config.Bucket, minio.ListObjectsOptions{Prefix: fullKey, Recursive: true}) {
		if object.Err != nil {
			hlog.CtxErrorf(ctx, "列出MinIO目录内容失败: %v", object.Err)
			return object.Err
		}

		// 删除每个对象
		if err := s.Delete(ctx, object.Key); err != nil {
			hlog.CtxErrorf(ctx, "删除MinIO对象失败: %v", err)
			return err
		}
	}

	hlog.CtxInfof(ctx, "成功从MinIO中删除目录及其所有内容: %s", fullKey)
	return nil
}

// ListDir 实现MinIO目录列表
func (s *MinIOStorage) ListDir(ctx context.Context, dirPath string) ([]FileMetadata, error) {
	hlog.CtxInfof(ctx, "开始列出MinIO目录内容: %s", dirPath)

	dirPath = ensureOSSDirPath(dirPath)
	fullKey := filepath.Join(s.config.BaseDir, dirPath)

	var fileMetas []FileMetadata

	// 获取目录下的所有对象
	for object := range s.client.ListObjects(ctx, s.config.Bucket, minio.ListObjectsOptions{Prefix: fullKey, Recursive: false}) {
		if object.Err != nil {
			hlog.CtxErrorf(ctx, "获取MinIO目录内容失败: %v", object.Err)
			return nil, object.Err
		}

		// 转换对象信息为FileMeta
		fileMeta := FileMetadata{
			Name:     object.Key[len(fullKey):], // 去除目录前缀
			Size:     object.Size,
			ModTime:  object.LastModified,
			IsDir:    object.Key[len(object.Key)-1] == '/',
			MIMEType: "application/octet-stream",
		}
		fileMetas = append(fileMetas, fileMeta)
	}

	hlog.CtxInfof(ctx, "成功列出MinIO目录内容: %s", dirPath)
	return fileMetas, nil
}

// GetMetadata 实现获取MinIO文件元数据
func (s *MinIOStorage) GetMetadata(ctx context.Context, filePath string) (*FileMetadata, error) {
	hlog.CtxInfof(ctx, "开始获取MinIO文件元数据: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	// 获取对象信息
	objectInfo, err := s.client.StatObject(ctx, s.config.Bucket, fullKey, minio.StatObjectOptions{})
	if err != nil {
		hlog.CtxErrorf(ctx, "获取MinIO文件信息失败: %v", err)
		return nil, err
	}

	// 构建元数据对象
	fileMeta := &FileMetadata{
		Name:     filePath,
		Size:     objectInfo.Size,
		ModTime:  objectInfo.LastModified,
		IsDir:    objectInfo.Key[len(objectInfo.Key)-1] == '/',
		MIMEType: "application/octet-stream",
	}

	hlog.CtxInfof(ctx, "成功获取MinIO文件元数据: %s", filePath)
	return fileMeta, nil
}

// UpdateMetadata 更新MinIO文件元数据（MinIO不支持直接更新元数据，除非重新上传文件）
func (s *MinIOStorage) UpdateMetadata(ctx context.Context, filePath string, metadata *FileMetadata) error {
	hlog.CtxInfof(ctx, "开始更新MinIO文件元数据: %s", filePath)
	hlog.CtxErrorf(ctx, "MinIO不支持直接更新元数据")
	return fmt.Errorf("MinIO不支持直接更新元数据")
}

// BatchUpload 实现MinIO批量上传
func (s *MinIOStorage) BatchUpload(ctx context.Context, files map[string]io.Reader) error {
	hlog.CtxInfof(ctx, "开始批量上传 %d 个文件到MinIO", len(files))

	for filePath, reader := range files {
		if err := s.Upload(ctx, filePath, reader); err != nil {
			hlog.CtxErrorf(ctx, "批量上传失败，文件: %s, 错误: %v", filePath, err)
			return err
		}
	}

	hlog.CtxInfof(ctx, "成功完成MinIO批量上传，共 %d 个文件", len(files))
	return nil
}

// BatchDownload 实现MinIO批量下载（流式下载）
func (s *MinIOStorage) BatchDownload(ctx context.Context, filePaths []string) (map[string]io.Reader, error) {
	hlog.CtxInfof(ctx, "开始批量下载 %d 个MinIO文件", len(filePaths))

	results := make(map[string]io.Reader)

	for _, filePath := range filePaths {
		reader, err := s.Download(ctx, filePath)
		if err != nil {
			hlog.CtxErrorf(ctx, "MinIO批量下载失败，文件: %s, 错误: %v", filePath, err)
			// 关闭已打开的reader
			for _, r := range results {
				if closer, ok := r.(io.Closer); ok {
					closer.Close()
				}
			}
			return nil, err
		}
		results[filePath] = reader
	}

	hlog.CtxInfof(ctx, "成功完成MinIO批量下载，共 %d 个文件", len(filePaths))
	return results, nil
}

// BatchDelete 实现MinIO批量删除
func (s *MinIOStorage) BatchDelete(ctx context.Context, filePaths []string) error {
	hlog.CtxInfof(ctx, "开始批量删除 %d 个MinIO文件", len(filePaths))

	for _, filePath := range filePaths {
		if err := s.Delete(ctx, filePath); err != nil {
			hlog.CtxErrorf(ctx, "批量删除失败，文件: %s, 错误: %v", filePath, err)
			return err
		}
	}

	hlog.CtxInfof(ctx, "成功完成MinIO批量删除，共 %d 个文件", len(filePaths))
	return nil
}
