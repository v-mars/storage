# Storage

一个支持多种存储后端的统一存储接口，包括本地存储、阿里云OSS和MinIO。该项目提供了一套统一的API来操作不同的存储系统，使应用程序可以轻松切换存储后端而无需修改业务逻辑。

## 功能特性

- 统一的存储接口，支持多种存储后端
- 支持本地文件系统存储
- 支持阿里云OSS存储
- 支持MinIO对象存储
- 支持文件上传、下载、删除、重命名、移动、复制等操作
- 支持目录操作（创建、删除、列表）
- 支持元数据管理
- 支持批量操作
- 支持断点续传下载

## 安装

```bash
go get github.com/v-mars/storage
```

## 使用方法

### 初始化存储实例

```go
import "github.com/v-mars/storage"

// 使用默认本地存储
storageConfig := &storage.Types{}
basePath, storageInstance := storageConfig.GetStorage(context.Background())

// 使用指定配置
storageConfig := &storage.Types{}
basePath, storageInstance := storageConfig.GetStorage(
    context.Background(),
    storage.WithMode(storage.OSS), // 或 storage.MinIO, storage.Local
    storage.WithOSSConfig(storage.OSSStorageConfig{
        Endpoint:        "your-endpoint",
        AccessKeyID:     "your-access-key-id",
        AccessKeySecret: "your-access-key-secret",
        Bucket:          "your-bucket",
        BaseDir:         "your-base-directory",
    }),
)
```

### 基本文件操作

```go
// 上传文件
file, _ := os.Open("example.txt")
err := storageInstance.Upload(context.Background(), "path/to/file.txt", file)

// 下载文件
reader, err := storageInstance.Download(context.Background(), "path/to/file.txt")
if err == nil {
    // 处理reader中的数据
    data, _ := io.ReadAll(reader)
    // ...
}

// 删除文件
err := storageInstance.Delete(context.Background(), "path/to/file.txt")

// 重命名文件
err := storageInstance.Rename(context.Background(), "old/path.txt", "new/path.txt")

// 移动文件
err := storageInstance.Move(context.Background(), "source/path.txt", "destination/path.txt")

// 复制文件
err := storageInstance.Copy(context.Background(), "source/path.txt", "destination/path.txt")
```

### 目录操作

```go
// 创建目录
err := storageInstance.CreateDir(context.Background(), "path/to/directory")

// 删除目录
err := storageInstance.DeleteDir(context.Background(), "path/to/directory")

// 列出目录内容
files, err := storageInstance.ListDir(context.Background(), "path/to/directory")
```

### 元数据操作

```go
// 获取文件元数据
metadata, err := storageInstance.GetMetadata(context.Background(), "path/to/file.txt")

// 更新文件元数据（部分存储后端支持）
err := storageInstance.UpdateMetadata(context.Background(), "path/to/file.txt", &storage.FileMetadata{
    ModTime: time.Now(),
})
```

### 批量操作

```go
// 批量上传
files := map[string]io.Reader{
    "file1.txt": reader1,
    "file2.txt": reader2,
}
err := storageInstance.BatchUpload(context.Background(), files)

// 批量下载
filePaths := []string{"file1.txt", "file2.txt"}
readers, err := storageInstance.BatchDownload(context.Background(), filePaths)

// 批量删除
filePaths := []string{"file1.txt", "file2.txt"}
err := storageInstance.BatchDelete(context.Background(), filePaths)
```

### 断点续传下载

```go
// 下载指定范围的数据
reader, err := storageInstance.DownloadRange(context.Background(), "path/to/file.txt", 100, 1024)
```

## 存储后端

### 本地存储 (Local)

将文件存储在本地文件系统中。需要配置基础路径。

### 阿里云OSS (OSS)

将文件存储在阿里云对象存储服务中。需要配置：
- Endpoint: OSS服务地址
- AccessKeyID: 访问密钥ID
- AccessKeySecret: 访问密钥密钥
- Bucket: 存储桶名称
- BaseDir: 基础目录

### MinIO

将文件存储在MinIO对象存储中。需要配置：
- Endpoint: MinIO服务地址
- AccessKeyID: 访问密钥ID
- AccessKeySecret: 访问密钥密钥
- UseSSL: 是否使用SSL连接
- Bucket: 存储桶名称
- BaseDir: 基础目录

## 接口定义

所有存储后端都实现了统一的[Storage](file:///Users/zhangdonghai/work/dev/go/storage/storage.go#L39-L53)接口：

```go
type Storage interface {
    // 基础操作
    Upload(ctx context.Context, filePath string, reader io.Reader) error
    Download(ctx context.Context, filePath string) (io.Reader, error)
    DownloadRange(ctx context.Context, filePath string, offset, size int64) (io.Reader, error)
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
    BatchDownload(ctx context.Context, filePaths []string) (map[string]io.Reader, error)
    BatchDelete(ctx context.Context, filePaths []string) error
}
```

## 单元测试

项目包含了针对本地存储的单元测试。可以通过以下命令运行测试：

```bash
go test -v
```

测试覆盖了以下功能：
- 文件上传
- 文件下载
- 文件删除
- 文件重命名
- 目录创建
- 元数据获取
- 批量上传
- 批量删除

## 命令行工具

项目包含一个命令行工具，用于演示如何使用存储接口。

### 安装

```bash
go install github.com/v-mars/storage/cmd/storage-cli@latest
```

### 使用方法

```bash
# 上传文件到本地存储
storage-cli -type=local -action=upload -src=/path/to/local/file.txt -dst=file.txt

# 下载文件
storage-cli -type=local -action=download -src=file.txt -dst=/path/to/local/downloaded.txt

# 删除文件
storage-cli -type=local -action=delete -src=file.txt

# 列出目录内容
storage-cli -type=local -action=list -dir=/

# 创建目录
storage-cli -type=local -action=mkdir -dir=new_directory

# 删除目录
storage-cli -type=local -action=rmdir -dir=new_directory

# 重命名文件
storage-cli -type=local -action=rename -src=old_name.txt -dst=new_name.txt
```

### 使用OSS存储

```bash
storage-cli \
  -type=oss \
  -oss.endpoint=your-oss-endpoint \
  -oss.accesskeyid=your-access-key-id \
  -oss.accesskeysecret=your-access-key-secret \
  -oss.bucket=your-bucket \
  -oss.basedir=your-base-dir \
  -action=upload \
  -src=/path/to/local/file.txt \
  -dst=file.txt
```

### 使用MinIO存储

```bash
storage-cli \
  -type=minio \
  -minio.endpoint=your-minio-endpoint \
  -minio.accesskeyid=your-access-key-id \
  -minio.accesskeysecret=your-access-key-secret \
  -minio.bucket=your-bucket \
  -minio.basedir=your-base-dir \
  -action=upload \
  -src=/path/to/local/file.txt \
  -dst=file.txt
```

## 许可证

MIT