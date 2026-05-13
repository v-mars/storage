package storage

import (
	"context"
	"fmt"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"path/filepath"
	"strings"
	"time"
)

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

// Upload 实现MinIO文件上传，支持设置有效期
func (s *MinIOStorage) Upload(ctx context.Context, filePath string, reader io.Reader, opts ...UploadOption) error {
	hlog.CtxInfof(ctx, "开始上传文件到MinIO: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	// 应用上传选项
	options := ApplyUploadOptions(opts...)

	putOpts := minio.PutObjectOptions{}

	// 如果设置了有效期，添加过期时间选项
	if options.Expiration > 0 {
		expiration := time.Now().Add(options.Expiration)
		putOpts.Expires = expiration
		hlog.CtxDebugf(ctx, "设置MinIO文件过期时间: %v", expiration)
	}

	// 使用流式上传
	_, err := s.client.PutObject(ctx, s.config.Bucket, fullKey, reader, -1, putOpts)
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
func (s *MinIOStorage) BatchUpload(ctx context.Context, files map[string]io.Reader, opts ...UploadOption) error {
	hlog.CtxInfof(ctx, "开始批量上传 %d 个文件到MinIO", len(files))

	for filePath, reader := range files {
		if err := s.Upload(ctx, filePath, reader, opts...); err != nil {
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
