package storage

import (
	"context"
	"io"
)

// BatchUploadHelper 提供批量上传的通用实现
func BatchUploadHelper(ctx context.Context, s Storage, files map[string]io.Reader, opts ...UploadOption) error {
	for filePath, reader := range files {
		if err := s.Upload(ctx, filePath, reader, opts...); err != nil {
			return err
		}
	}
	return nil
}

// BatchDownloadHelper 提供批量下载的通用实现
func BatchDownloadHelper(ctx context.Context, s Storage, filePaths []string) (map[string]io.Reader, error) {
	results := make(map[string]io.Reader)
	for _, filePath := range filePaths {
		reader, err := s.Download(ctx, filePath)
		if err != nil {
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
	return results, nil
}

// BatchDeleteHelper 提供批量删除的通用实现
func BatchDeleteHelper(ctx context.Context, s Storage, filePaths []string) error {
	for _, filePath := range filePaths {
		if err := s.Delete(ctx, filePath); err != nil {
			return err
		}
	}
	return nil
}