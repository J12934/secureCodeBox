package presigned_file_storage

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestS3FileStorageConfig(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FileStorage Suite")
}
