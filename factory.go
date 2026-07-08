package storage

import (
	"context"

	"github.com/cloudwego/hertz/pkg/common/hlog"
)

type Types struct {
	Mode       StorageType        `yaml:"mode" json:"mode"`               // local, s3, minio, oss, cos,
	AssignMode StorageType        `yaml:"assign_mode" json:"assign_mode"` // local, s3, minio, oss, cos,
	MaxSize    int64              `yaml:"max_size" json:"max_size"`
	Local      LocalStorageConfig `json:"local"`
	Minio      MinIOStorageConfig `json:"minio"`
	Oss        OSSStorageConfig   `json:"oss"`
	S3         S3StorageConfig    `json:"s3"`
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

// WithS3Config 设置S3存储配置选项
func WithS3Config(config S3StorageConfig) StorageOption {
	return func(s *Types) {
		s.S3 = config
		s.Mode = S3
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
			BasePath: "/tmp", // 默认使用系统临时目录
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
		for _, opt := range DefaultStorageOptions() {
			opt(s)
		}
		s.AssignMode = s.Mode
	}

	// 根据模式返回相应的存储实例
	switch s.AssignMode {
	case S3:
		// 验证S3配置
		if s.S3.BaseDir == "" || s.S3.Endpoint == "" || s.S3.AccessKeyID == "" || s.S3.AccessKeySecret == "" || s.S3.Bucket == "" || s.S3.Region == "" {
			hlog.CtxErrorf(ctx, "S3 config error: missing required fields")
			return "", nil
		}
		hlog.CtxInfof(ctx, "Using S3 storage")
		return s.S3.BaseDir, NewS3Storage(s.S3)
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

	// 注册S3存储驱动（示例配置）
	//RegisterStorageDriver(S3, func() Storage {
	//	// 实际使用时应从安全的配置源获取
	//	return NewS3Storage(S3StorageConfig{
	//		Endpoint:        "https://s3.amazonaws.com",
	//		AccessKeyID:     "your-s3-access-key-id",
	//		AccessKeySecret: "your-s3-access-key-secret",
	//		Region:          "us-east-1",
	//		UseSSL:          true,
	//		Bucket:          "your-s3-bucket",
	//		BaseDir:         "your-s3-base-dir",
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
