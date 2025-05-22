package presigned_file_storage

import (
	"time"

	executionv1 "github.com/secureCodeBox/secureCodeBox/operator/apis/execution/v1"
)

type FileStorage interface {
	// PresignedGetURL returns a presigned URL for getting a file from the storage.
	PresignedGetURL(scan executionv1.Scan, filename string, duration time.Duration) (string, error)
	// PresignedPutURL returns a presigned URL for putting a file into the storage.
	PresignedPutURL(scan executionv1.Scan, filename string, duration time.Duration) (string, error)
	// PresignedHeadURL returns a presigned URL for getting the metadata of a file from the storage.
	PresignedHeadURL(scan executionv1.Scan, filename string, duration time.Duration) (string, error)

	// DeleteFile deletes a file from the storage.
	DeleteFile(scan executionv1.Scan, filename string) error
}
