package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/v-mars/storage"
	"io"
	"os"
	"path/filepath"
	_ "strings"
)

var (
	storageType = flag.String("type", "local", "Storage type: local, oss, minio")
	action      = flag.String("action", "", "Action to perform: upload, download, delete, list, mkdir, rmdir")
	src         = flag.String("src", "", "Source file path")
	dst         = flag.String("dst", "", "Destination file path")
	dir         = flag.String("dir", "", "Directory path")
	configFile  = flag.String("config", "", "Configuration file path")

	// Local storage options
	localBasePath = flag.String("local.basepath", os.TempDir(), "Local storage base path")

	// OSS storage options
	ossEndpoint        = flag.String("oss.endpoint", "", "OSS endpoint")
	ossAccessKeyID     = flag.String("oss.accesskeyid", "", "OSS access key ID")
	ossAccessKeySecret = flag.String("oss.accesskeysecret", "", "OSS access key secret")
	ossBucket          = flag.String("oss.bucket", "", "OSS bucket name")
	ossBaseDir         = flag.String("oss.basedir", "", "OSS base directory")

	// MinIO storage options
	minioEndpoint        = flag.String("minio.endpoint", "", "MinIO endpoint")
	minioAccessKeyID     = flag.String("minio.accesskeyid", "", "MinIO access key ID")
	minioAccessKeySecret = flag.String("minio.accesskeysecret", "", "MinIO access key secret")
	minioUseSSL          = flag.Bool("minio.usessl", false, "MinIO use SSL")
	minioBucket          = flag.String("minio.bucket", "", "MinIO bucket name")
	minioBaseDir         = flag.String("minio.basedir", "", "MinIO base directory")
)

func main() {
	flag.Parse()

	if *action == "" {
		fmt.Println("Error: action is required")
		flag.Usage()
		os.Exit(1)
	}

	// 初始化存储配置
	storageConfig := &storage.Types{}

	// 根据存储类型设置配置
	switch storage.StorageType(*storageType) {
	case storage.Local:
		storageConfig.Local = storage.LocalStorageConfig{
			BasePath: *localBasePath,
		}
	case storage.OSS:
		storageConfig.Oss = storage.OSSStorageConfig{
			Endpoint:        *ossEndpoint,
			AccessKeyID:     *ossAccessKeyID,
			AccessKeySecret: *ossAccessKeySecret,
			Bucket:          *ossBucket,
			BaseDir:         *ossBaseDir,
		}
	case storage.MinIO:
		storageConfig.Minio = storage.MinIOStorageConfig{
			Endpoint:        *minioEndpoint,
			AccessKeyID:     *minioAccessKeyID,
			AccessKeySecret: *minioAccessKeySecret,
			UseSSL:          *minioUseSSL,
			Bucket:          *minioBucket,
			BaseDir:         *minioBaseDir,
		}
	default:
		storageConfig.Local = storage.LocalStorageConfig{
			BasePath: *localBasePath,
		}
	}

	// 获取存储实例
	basePath, storageInstance := storageConfig.GetStorage(
		context.Background(),
		storage.WithMode(storage.StorageType(*storageType)),
	)

	if storageInstance == nil {
		fmt.Printf("Failed to initialize storage instance for type: %s\n", *storageType)
		os.Exit(1)
	}

	fmt.Printf("Using storage type: %s, base path: %s\n", *storageType, basePath)

	// 执行操作
	ctx := context.Background()
	switch *action {
	case "upload":
		if *src == "" || *dst == "" {
			fmt.Println("Error: src and dst are required for upload action")
			os.Exit(1)
		}
		uploadFile(ctx, storageInstance, *src, *dst)
	case "download":
		if *src == "" || *dst == "" {
			fmt.Println("Error: src and dst are required for download action")
			os.Exit(1)
		}
		downloadFile(ctx, storageInstance, *src, *dst)
	case "delete":
		if *src == "" {
			fmt.Println("Error: src is required for delete action")
			os.Exit(1)
		}
		deleteFile(ctx, storageInstance, *src)
	case "list":
		listDir(ctx, storageInstance, *dir)
	case "mkdir":
		if *dir == "" {
			fmt.Println("Error: dir is required for mkdir action")
			os.Exit(1)
		}
		createDir(ctx, storageInstance, *dir)
	case "rmdir":
		if *dir == "" {
			fmt.Println("Error: dir is required for rmdir action")
			os.Exit(1)
		}
		deleteDir(ctx, storageInstance, *dir)
	case "rename":
		if *src == "" || *dst == "" {
			fmt.Println("Error: src and dst are required for rename action")
			os.Exit(1)
		}
		renameFile(ctx, storageInstance, *src, *dst)
	default:
		fmt.Printf("Error: unsupported action: %s\n", *action)
		os.Exit(1)
	}
}

func uploadFile(ctx context.Context, s storage.Storage, srcPath, dstPath string) {
	file, err := os.Open(srcPath)
	if err != nil {
		fmt.Printf("Failed to open source file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	err = s.Upload(ctx, dstPath, file)
	if err != nil {
		fmt.Printf("Failed to upload file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully uploaded %s to %s\n", srcPath, dstPath)
}

func downloadFile(ctx context.Context, s storage.Storage, srcPath, dstPath string) {
	reader, err := s.Download(ctx, srcPath)
	if err != nil {
		fmt.Printf("Failed to download file: %v\n", err)
		os.Exit(1)
	}

	// 创建目标目录
	tmpdir := filepath.Dir(dstPath)
	if err = os.MkdirAll(tmpdir, 0755); err != nil {
		fmt.Printf("Failed to create directory: %v\n", err)
		os.Exit(1)
	}

	// 创建目标文件
	file, err := os.Create(dstPath)
	if err != nil {
		fmt.Printf("Failed to create destination file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// 写入文件内容
	_, err = io.Copy(file, reader)
	if err != nil {
		fmt.Printf("Failed to write to destination file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully downloaded %s to %s\n", srcPath, dstPath)
}

func deleteFile(ctx context.Context, s storage.Storage, filePath string) {
	err := s.Delete(ctx, filePath)
	if err != nil {
		fmt.Printf("Failed to delete file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully deleted %s\n", filePath)
}

func listDir(ctx context.Context, s storage.Storage, dirPath string) {
	files, err := s.ListDir(ctx, dirPath)
	if err != nil {
		fmt.Printf("Failed to list directory: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Contents of %s:\n", dirPath)
	for _, file := range files {
		fileType := "file"
		if file.IsDir {
			fileType = "dir"
		}
		fmt.Printf("%s\t%s\t%d bytes\t%s\n", fileType, file.Name, file.Size, file.ModTime.Format("2006-01-02 15:04:05"))
	}
}

func createDir(ctx context.Context, s storage.Storage, dirPath string) {
	err := s.CreateDir(ctx, dirPath)
	if err != nil {
		fmt.Printf("Failed to create directory: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully created directory %s\n", dirPath)
}

func deleteDir(ctx context.Context, s storage.Storage, dirPath string) {
	err := s.DeleteDir(ctx, dirPath)
	if err != nil {
		fmt.Printf("Failed to delete directory: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully deleted directory %s\n", dirPath)
}

func renameFile(ctx context.Context, s storage.Storage, srcPath, dstPath string) {
	err := s.Rename(ctx, srcPath, dstPath)
	if err != nil {
		fmt.Printf("Failed to rename file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully renamed %s to %s\n", srcPath, dstPath)
}
