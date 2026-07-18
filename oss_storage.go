package storage

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/cloudwego/hertz/pkg/common/hlog"
)

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

// Upload 实现OSS文件上传，支持设置有效期
func (s *OSSStorage) Upload(ctx context.Context, filePath string, reader io.Reader, opts ...UploadOption) error {
	hlog.CtxInfof(ctx, "开始上传文件到OSS: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	// 应用上传选项
	options := ApplyUploadOptions(opts...)

	var putOptions []oss.Option

	// 如果设置了有效期，添加过期时间选项
	if options.Expiration > 0 {
		expiration := time.Now().Add(options.Expiration)
		putOptions = append(putOptions, oss.Expires(expiration))
		hlog.CtxDebugf(ctx, "设置OSS文件过期时间: %v", expiration)
	}

	err := s.bucket.PutObject(fullKey, reader, putOptions...)
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

	body, err := s.bucket.GetObject(fullKey, oss.Range(offset, offset+size-1))
	if err != nil {
		hlog.CtxErrorf(ctx, "OSS获取文件范围失败: %v", err)
		return nil, err
	}

	hlog.CtxInfof(ctx, "OSS文件断点续传下载已启动: %s", filePath)
	return body, nil // 返回原始的Reader，由调用方负责关闭
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

// Exists 实现检查OSS文件是否存在
func (s *OSSStorage) Exists(ctx context.Context, filePath string) (bool, error) {
	fullKey := filepath.Join(s.config.BaseDir, filePath)
	return s.bucket.IsObjectExist(fullKey)
}

// CreateDir 在OSS中创建目录。
// 对象存储中目录是隐式的，无需显式创建占位对象。
func (s *OSSStorage) CreateDir(ctx context.Context, dirPath string) error {
	hlog.CtxDebugf(ctx, "OSS 目录无需显式创建: %s", dirPath)
	return nil
}

// DeleteDir 从OSS中删除目录（递归删除目录下所有对象）
func (s *OSSStorage) DeleteDir(ctx context.Context, dirPath string) error {
	hlog.CtxInfof(ctx, "开始从OSS中删除目录及其所有内容: %s", dirPath)

	dirPath = ensureOSSDirPath(dirPath)
	fullKey := joinStorageKey(s.config.BaseDir, dirPath)

	// 获取目录下的所有对象并删除
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

		// 执行列表操作
		objectListing, err := s.bucket.ListObjects(listOptions...)
		if err != nil {
			hlog.CtxErrorf(ctx, "列出OSS目录内容失败: %v", err)
			return fmt.Errorf("列出OSS目录内容失败：%v", err)
		}

		// 删除目录下的所有对象（直接调用底层API，object.Key已经是完整路径）
		for _, object := range objectListing.Objects {
			if strings.HasPrefix(object.Key, fullKey) && !isDirectoryPlaceholder(object.Key, fullKey) {
				if err = s.bucket.DeleteObject(object.Key); err != nil {
					hlog.CtxErrorf(ctx, "删除OSS对象失败: %v", err)
					return fmt.Errorf("删除OSS对象失败：%v", err)
				}
			}
		}

		// 如果当前批次返回的对象数量少于最大限制，则认为是最后一批
		if len(objectListing.Objects) < 1000 {
			break
		}

		// 使用最后一个对象的Key作为下次查询的marker
		marker = objectListing.NextMarker
	}

	hlog.CtxInfof(ctx, "成功从OSS中删除目录及其所有内容: %s", fullKey)
	return nil
}

// ListDir 列出OSS目录内容。
// OSS 默认 ListObjects 不带 Delimiter，会递归返回当前目录下所有对象，
// 调用方（如 FileManager.walkDir）需自行处理层级关系。
func (s *OSSStorage) ListDir(ctx context.Context, dirPath string) ([]FileMetadata, error) {
	hlog.CtxInfof(ctx, "开始列出OSS目录内容: %s", dirPath)

	// 必须保证 prefix 以 / 结尾，避免把 story-script-demo-other 等相似前缀也匹配进来。
	dirPath = ensureOSSDirPath(dirPath)
	fullKey := joinStorageKey(s.config.BaseDir, dirPath)

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
				name := object.Key[len(fullKey):]
				if name == "" {
					continue
				}
				fileMeta := FileMetadata{
					Name:     name,
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
		modTime, err = time.Parse("2006-01-02 15:04:05", modTimeStr)
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
func (s *OSSStorage) BatchUpload(ctx context.Context, files map[string]io.Reader, opts ...UploadOption) error {
	hlog.CtxInfof(ctx, "开始批量上传 %d 个文件到OSS", len(files))
	return BatchUploadHelper(ctx, s, files, opts...)
}

// BatchDownload 实现OSS批量下载（流式下载）
func (s *OSSStorage) BatchDownload(ctx context.Context, filePaths []string) (map[string]io.Reader, error) {
	hlog.CtxInfof(ctx, "开始批量下载 %d 个OSS文件", len(filePaths))
	return BatchDownloadHelper(ctx, s, filePaths)
}

// BatchDelete 实现OSS批量删除
func (s *OSSStorage) BatchDelete(ctx context.Context, filePaths []string) error {
	hlog.CtxInfof(ctx, "开始批量删除 %d 个OSS文件", len(filePaths))
	return BatchDeleteHelper(ctx, s, filePaths)
}
