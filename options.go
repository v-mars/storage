package storage

import (
	"time"
)

// UploadOption 定义上传选项函数类型
type UploadOption func(*UploadOptions)

// UploadOptions 上传选项配置
type UploadOptions struct {
	Expiration time.Duration // 文件有效期（仅OSS和MinIO支持）
}

// WithExpiration 设置文件有效期选项
func WithExpiration(expiration time.Duration) UploadOption {
	return func(opts *UploadOptions) {
		opts.Expiration = expiration
	}
}

// DefaultUploadOptions 默认上传选项
func DefaultUploadOptions() *UploadOptions {
	return &UploadOptions{}
}

// ApplyUploadOptions 应用上传选项
func ApplyUploadOptions(opts ...UploadOption) *UploadOptions {
	options := DefaultUploadOptions()
	for _, opt := range opts {
		opt(options)
	}
	return options
}
