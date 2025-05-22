package presigned_file_storage

import (
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ParseS3Config", func() {
	AfterEach(func() {
		os.Unsetenv("S3_ENDPOINT")
		os.Unsetenv("S3_BUCKET")
		os.Unsetenv("S3_AUTH_TYPE")
		os.Unsetenv("S3_USE_SSL")
		os.Unsetenv("S3_PORT")
		os.Unsetenv("S3_URL_TEMPLATE")
		os.Unsetenv("S3_AWS_IRSA_STS_ENDPOINT")
	})

	It("parses a valid access-and-secret-key config", func() {
		os.Setenv("S3_ENDPOINT", "s3.example.com")
		os.Setenv("S3_BUCKET", "test-bucket")
		os.Setenv("S3_AUTH_TYPE", "access-and-secret-key")
		os.Setenv("S3_USE_SSL", "true")
		os.Setenv("S3_PORT", "9000")
		os.Setenv("S3_URL_TEMPLATE", "custom/{{ .Scan.UID }}/{{ .Filename }}")

		cfg, err := ParseS3Config()
		Expect(err).To(BeNil())
		Expect(cfg.Endpoint).To(Equal("s3.example.com"))
		Expect(cfg.Bucket).To(Equal("test-bucket"))
		Expect(cfg.AuthType).To(Equal(AuthTypeAccessAndSecretKey))
		Expect(cfg.UseSSL).To(BeTrue())
		Expect(cfg.Port).To(Equal("9000"))
		Expect(cfg.UrlTemplate).To(Equal("custom/{{ .Scan.UID }}/{{ .Filename }}"))
	})

	It("parses a valid aws-irsa config", func() {
		os.Setenv("S3_ENDPOINT", "s3.aws.com")
		os.Setenv("S3_BUCKET", "aws-bucket")
		os.Setenv("S3_AUTH_TYPE", "aws-irsa")
		os.Setenv("S3_USE_SSL", "false")
		os.Setenv("S3_AWS_IRSA_STS_ENDPOINT", "sts.aws.com")
		os.Unsetenv("S3_URL_TEMPLATE")

		cfg, err := ParseS3Config()
		Expect(err).To(BeNil())
		Expect(cfg.Endpoint).To(Equal("s3.aws.com"))
		Expect(cfg.Bucket).To(Equal("aws-bucket"))
		Expect(cfg.AuthType).To(Equal(AuthTypeAWSIRSA))
		Expect(cfg.UseSSL).To(BeFalse())
		Expect(cfg.StsEndpoint).To(Equal("sts.aws.com"))
		Expect(cfg.UrlTemplate).To(Equal("scan-{{ .Scan.UID }}/{{ .Filename }}"))
	})

	It("returns an error if required fields are missing", func() {
		os.Unsetenv("S3_ENDPOINT")
		os.Unsetenv("S3_BUCKET")
		os.Setenv("S3_AUTH_TYPE", "access-and-secret-key")
		cfg, err := ParseS3Config()
		Expect(cfg).To(BeNil())
		Expect(err).To(MatchError("S3_ENDPOINT is required"))

		os.Setenv("S3_ENDPOINT", "s3.example.com")
		os.Unsetenv("S3_BUCKET")
		cfg, err = ParseS3Config()
		Expect(cfg).To(BeNil())
		Expect(err).To(MatchError("S3_BUCKET is required"))
	})
})
