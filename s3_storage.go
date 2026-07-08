package storage

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cloudwego/hertz/pkg/common/hlog"
)

// S3StorageConfig S3 存储配置
type S3StorageConfig struct {
	Endpoint        string `json:"endpoint"`          // S3 endpoint
	AccessKeyID     string `json:"access_key_id"`     // Access Key ID
	AccessKeySecret string `json:"access_key_secret"` // Access Key Secret
	Region          string `json:"region"`            // AWS区域
	UseSSL          bool   `json:"use_ssl"`           // 是否使用SSL
	Bucket          string `json:"bucket"`            // 存储桶名称
	BaseDir         string `json:"base_dir"`          // 存储基础目录
}

// S3Storage S3 存储实现
type S3Storage struct {
	config S3StorageConfig
	client *s3.Client
}

// NewS3Storage 创建新的S3存储实例
func NewS3Storage(cfg S3StorageConfig) Storage {
	// 创建AWS配置
	awsCfg, err := awscfg.LoadDefaultConfig(context.TODO(),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.AccessKeySecret,
			"",
		)),
		awscfg.WithRegion(cfg.Region),
	)
	if err != nil {
		hlog.Errorf("加载AWS配置失败: %v", err)
		return nil
	}

	// 创建S3客户端选项
	clientOptions := func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true // 使用路径样式访问（兼容MinIO等）
	}

	// 创建S3客户端
	client := s3.NewFromConfig(awsCfg, clientOptions)

	// 检查Bucket是否存在，如果不存在则创建
	_, err = client.HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: aws.String(cfg.Bucket),
	})
	if err != nil {
		// Bucket不存在，尝试创建
		_, createErr := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: aws.String(cfg.Bucket),
		})
		if createErr != nil {
			hlog.Errorf("创建S3 Bucket失败: %v", createErr)
			return nil
		}
		hlog.Infof("成功创建新S3 Bucket: %s", cfg.Bucket)
	}

	return &S3Storage{
		config: cfg,
		client: client,
	}
}

// Upload 实现S3文件上传，支持设置有效期
func (s *S3Storage) Upload(ctx context.Context, filePath string, reader io.Reader, opts ...UploadOption) error {
	hlog.CtxInfof(ctx, "开始上传文件到S3: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	// 应用上传选项
	options := ApplyUploadOptions(opts...)

	input := &s3.PutObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullKey),
		Body:   reader,
	}

	// 如果设置了有效期，添加过期时间
	if options.Expiration > 0 {
		expiration := time.Now().Add(options.Expiration)
		input.Expires = aws.Time(expiration)
		hlog.CtxDebugf(ctx, "设置S3文件过期时间: %v", expiration)
	}

	// 使用流式上传
	_, err := s.client.PutObject(ctx, input)
	if err != nil {
		hlog.CtxErrorf(ctx, "S3上传文件失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "S3文件上传成功: %s", filePath)
	return nil
}

// Download 实现从S3下载文件（流式下载）
func (s *S3Storage) Download(ctx context.Context, filePath string) (io.Reader, error) {
	hlog.CtxInfof(ctx, "开始从S3下载文件: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		hlog.CtxErrorf(ctx, "S3获取文件失败: %v", err)
		return nil, err
	}

	hlog.CtxInfof(ctx, "S3文件下载已启动: %s", filePath)
	return output.Body, nil // 返回原始的Reader，由调用方负责关闭
}

// DownloadRange 实现从S3下载文件（支持断点续传）
func (s *S3Storage) DownloadRange(ctx context.Context, filePath string, offset int64, size int64) (io.Reader, error) {
	hlog.CtxInfof(ctx, "开始从S3下载文件: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)

	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullKey),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		hlog.CtxErrorf(ctx, "S3获取文件范围失败: %v", err)
		return nil, err
	}

	hlog.CtxInfof(ctx, "S3文件断点续传下载已启动: %s", filePath)
	return output.Body, nil
}

// Delete 实现S3文件删除
func (s *S3Storage) Delete(ctx context.Context, filePath string) error {
	hlog.CtxInfof(ctx, "开始从S3删除文件: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		hlog.CtxErrorf(ctx, "S3删除文件失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "S3文件删除成功: %s", filePath)
	return nil
}

// Rename 实现S3文件重命名（复制+删除）
func (s *S3Storage) Rename(ctx context.Context, oldPath string, newPath string) error {
	hlog.CtxInfof(ctx, "开始在S3中重命名文件: %s -> %s", oldPath, newPath)

	oldFullKey := filepath.Join(s.config.BaseDir, oldPath)
	newFullKey := filepath.Join(s.config.BaseDir, newPath)

	// 复制文件到新路径
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.config.Bucket),
		Key:        aws.String(newFullKey),
		CopySource: aws.String(url.PathEscape(s.config.Bucket + "/" + oldFullKey)),
	})
	if err != nil {
		hlog.CtxErrorf(ctx, "S3复制文件失败: %v", err)
		return err
	}

	// 删除旧文件
	if err = s.Delete(ctx, oldPath); err != nil {
		hlog.CtxErrorf(ctx, "S3删除旧文件失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "S3文件重命名成功: %s -> %s", oldPath, newPath)
	return nil
}

// Move 实现S3文件移动（与重命名相同的操作）
func (s *S3Storage) Move(ctx context.Context, srcPath string, dstPath string) error {
	hlog.CtxInfof(ctx, "开始在S3中移动文件: %s -> %s", srcPath, dstPath)
	return s.Rename(ctx, srcPath, dstPath)
}

// Copy 实现S3文件复制
func (s *S3Storage) Copy(ctx context.Context, srcPath string, dstPath string) error {
	hlog.CtxInfof(ctx, "开始在S3中复制文件: %s -> %s", srcPath, dstPath)

	srcFullKey := filepath.Join(s.config.BaseDir, srcPath)
	dstFullKey := filepath.Join(s.config.BaseDir, dstPath)

	// 复制文件
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.config.Bucket),
		Key:        aws.String(dstFullKey),
		CopySource: aws.String(url.PathEscape(s.config.Bucket + "/" + srcFullKey)),
	})
	if err != nil {
		hlog.CtxErrorf(ctx, "S3复制文件失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "S3文件复制成功: %s -> %s", srcPath, dstPath)
	return nil
}

// Exists 实现检查S3文件是否存在
func (s *S3Storage) Exists(ctx context.Context, filePath string) (bool, error) {
	fullKey := filepath.Join(s.config.BaseDir, filePath)
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		return false, nil
	}
	return true, nil
}

// CreateDir 实现S3目录创建（通过创建以/结尾的对象模拟目录）
func (s *S3Storage) CreateDir(ctx context.Context, dirPath string) error {
	hlog.CtxInfof(ctx, "开始在S3中创建目录: %s", dirPath)

	dirPath = ensureOSSDirPath(dirPath)
	fullKey := filepath.Join(s.config.BaseDir, dirPath)

	// 检查目录是否已存在
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullKey),
	})
	if err == nil {
		hlog.CtxDebugf(ctx, "S3目录已存在，无需创建: %s", fullKey)
		return nil
	}

	// 创建空对象作为目录占位符
	if err = s.Upload(ctx, dirPath, strings.NewReader("")); err != nil {
		hlog.CtxErrorf(ctx, "S3创建目录占位符失败: %v", err)
		return err
	}

	hlog.CtxInfof(ctx, "S3目录创建成功: %s", fullKey)
	return nil
}

// DeleteDir 实现S3目录删除（递归删除目录下所有对象）
func (s *S3Storage) DeleteDir(ctx context.Context, dirPath string) error {
	hlog.CtxInfof(ctx, "开始从S3中删除目录及其所有内容: %s", dirPath)

	dirPath = ensureOSSDirPath(dirPath)
	fullKey := filepath.Join(s.config.BaseDir, dirPath)

	// 列出目录下的所有对象并删除（直接使用底层client，避免key重复拼接baseDir）
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
		Prefix: aws.String(fullKey),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			hlog.CtxErrorf(ctx, "列出S3目录内容失败: %v", err)
			return err
		}

		for _, object := range page.Contents {
			// 直接调用底层API，object.Key已经是完整路径
			_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(s.config.Bucket),
				Key:    object.Key,
			})
			if err != nil {
				hlog.CtxErrorf(ctx, "删除S3对象失败: %v", err)
				return err
			}
		}
	}

	hlog.CtxInfof(ctx, "成功从S3中删除目录及其所有内容: %s", fullKey)
	return nil
}

// ListDir 实现S3目录列表
func (s *S3Storage) ListDir(ctx context.Context, dirPath string) ([]FileMetadata, error) {
	hlog.CtxInfof(ctx, "开始列出S3目录内容: %s", dirPath)

	dirPath = ensureOSSDirPath(dirPath)
	fullKey := filepath.Join(s.config.BaseDir, dirPath)

	var fileMetas []FileMetadata

	// 获取目录下的所有对象
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.config.Bucket),
		Prefix:    aws.String(fullKey),
		Delimiter: aws.String("/"),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			hlog.CtxErrorf(ctx, "获取S3目录内容失败: %v", err)
			return nil, err
		}

		// 处理普通对象
		for _, object := range page.Contents {
			fileMeta := FileMetadata{
				Name:     (*object.Key)[len(fullKey):], // 去除目录前缀
				Size:     *object.Size,
				ModTime:  *object.LastModified,
				IsDir:    false,
				MIMEType: "application/octet-stream",
			}
			fileMetas = append(fileMetas, fileMeta)
		}

		// 处理子目录（CommonPrefixes）
		for _, prefix := range page.CommonPrefixes {
			prefixName := (*prefix.Prefix)[len(fullKey):]
			fileMeta := FileMetadata{
				Name:     prefixName,
				Size:     0,
				ModTime:  time.Time{},
				IsDir:    true,
				MIMEType: "",
			}
			fileMetas = append(fileMetas, fileMeta)
		}
	}

	hlog.CtxInfof(ctx, "成功列出S3目录内容: %s", dirPath)
	return fileMetas, nil
}

// GetMetadata 实现获取S3文件元数据
func (s *S3Storage) GetMetadata(ctx context.Context, filePath string) (*FileMetadata, error) {
	hlog.CtxInfof(ctx, "开始获取S3文件元数据: %s", filePath)

	fullKey := filepath.Join(s.config.BaseDir, filePath)

	// 获取对象信息
	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		hlog.CtxErrorf(ctx, "获取S3文件信息失败: %v", err)
		return nil, err
	}

	// 构建元数据对象
	fileMeta := &FileMetadata{
		Name:     filePath,
		Size:     *output.ContentLength,
		ModTime:  *output.LastModified,
		IsDir:    false,
		MIMEType: aws.ToString(output.ContentType),
	}

	hlog.CtxInfof(ctx, "成功获取S3文件元数据: %s", filePath)
	return fileMeta, nil
}

// UpdateMetadata 更新S3文件元数据（S3不支持直接更新元数据，除非重新上传文件）
func (s *S3Storage) UpdateMetadata(ctx context.Context, filePath string, metadata *FileMetadata) error {
	hlog.CtxInfof(ctx, "开始更新S3文件元数据: %s", filePath)
	hlog.CtxErrorf(ctx, "S3不支持直接更新元数据")
	return fmt.Errorf("S3不支持直接更新元数据")
}

// BatchUpload 实现S3批量上传
func (s *S3Storage) BatchUpload(ctx context.Context, files map[string]io.Reader, opts ...UploadOption) error {
	hlog.CtxInfof(ctx, "开始批量上传 %d 个文件到S3", len(files))
	return BatchUploadHelper(ctx, s, files, opts...)
}

// BatchDownload 实现S3批量下载（流式下载）
func (s *S3Storage) BatchDownload(ctx context.Context, filePaths []string) (map[string]io.Reader, error) {
	hlog.CtxInfof(ctx, "开始批量下载 %d 个S3文件", len(filePaths))
	return BatchDownloadHelper(ctx, s, filePaths)
}

// BatchDelete 实现S3批量删除
func (s *S3Storage) BatchDelete(ctx context.Context, filePaths []string) error {
	hlog.CtxInfof(ctx, "开始批量删除 %d 个S3文件", len(filePaths))
	return BatchDeleteHelper(ctx, s, filePaths)
}
