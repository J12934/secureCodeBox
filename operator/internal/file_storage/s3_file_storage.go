package presigned_file_storage

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	executionv1 "github.com/secureCodeBox/secureCodeBox/operator/apis/execution/v1"
)

// AuthTypeEnum defines supported authentication types for S3
type AuthTypeEnum int

const (
	AuthTypeAccessAndSecretKey AuthTypeEnum = iota
	AuthTypeAWSIRSA
)

func (a AuthTypeEnum) String() string {
	switch a {
	case AuthTypeAWSIRSA:
		return "aws-irsa"
	default:
		return "access-and-secret-key"
	}
}

func ParseAuthType(s string) AuthTypeEnum {
	switch strings.ToLower(s) {
	case "aws-irsa":
		return AuthTypeAWSIRSA
	default:
		return AuthTypeAccessAndSecretKey
	}
}

type S3FileStorage struct {
	MinioClient *minio.Client
	Log         logr.Logger
	Config      *S3Config
}

type S3Config struct {
	// Endpoint is the S3 hostname or IP address e.g. s3.amazonaws.com
	Endpoint string
	// Port is the S3 port e.g. 443
	Port string
	// UseSSL is true if the S3 endpoint uses TLS
	UseSSL bool
	// Bucket is the S3 bucket name e.g. securecodebox-results
	Bucket string

	// UrlTemplate is the template to use for the S3 object URL, if not set, the default is "scan-{{ .Scan.UID }}/{{ .Filename }}"
	UrlTemplate string
	// AuthType is the authentication type to use for S3
	AuthType AuthTypeEnum
	// StsEndpoint is the STS endpoint to use for AWS IRSA authentication only relevant if AuthType is set to aws-irsa
	StsEndpoint string
}

func ParseS3Config() (*S3Config, error) {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		return nil, fmt.Errorf("S3_ENDPOINT is required")
	}
	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		return nil, fmt.Errorf("S3_BUCKET is required")
	}
	port := os.Getenv("S3_PORT")
	useSSL := true
	if os.Getenv("S3_USE_SSL") == "false" {
		useSSL = false
	}
	authTypeStr := os.Getenv("S3_AUTH_TYPE")
	authType := ParseAuthType(authTypeStr)

	stsEndpoint := os.Getenv("S3_AWS_IRSA_STS_ENDPOINT")
	urlTemplate := os.Getenv("S3_URL_TEMPLATE")
	if urlTemplate == "" {
		urlTemplate = "scan-{{ .Scan.UID }}/{{ .Filename }}"
	}

	return &S3Config{
		Endpoint:    endpoint,
		Port:        port,
		UseSSL:      useSSL,
		AuthType:    authType,
		StsEndpoint: stsEndpoint,
		Bucket:      bucket,
		UrlTemplate: urlTemplate,
	}, nil
}
func NewS3FileStorage(logger logr.Logger) (*S3FileStorage, error) {
	config, err := ParseS3Config()
	if err != nil {
		logger.Error(err, "Invalid S3 configuration")
		return nil, err
	}
	return NewS3FileStorageWithConfig(logger, config)
}

func NewS3FileStorageWithConfig(logger logr.Logger, config *S3Config) (*S3FileStorage, error) {
	var creds *credentials.Credentials
	switch config.AuthType {
	case AuthTypeAWSIRSA:
		stsEndpoint := config.StsEndpoint
		logger.Info("Using AWS IRSA ServiceAccount Bindung for S3 Authentication", "sts", stsEndpoint)
		creds = credentials.NewIAM(stsEndpoint)
	case AuthTypeAccessAndSecretKey:
		creds = credentials.NewEnvMinio()
	default:
		return nil, fmt.Errorf("unsupported S3_AUTH_TYPE: %v", config.AuthType)
	}

	endpoint := config.Endpoint
	if config.Port != "" {
		endpoint = fmt.Sprintf("%s:%s", endpoint, config.Port)
	}
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  creds,
		Secure: config.UseSSL,
	})
	if err != nil {
		logger.Error(err, "Could not create minio client to communicate with s3 or compatible storage provider")
		return nil, err
	}

	return &S3FileStorage{
		MinioClient: minioClient,
		Log:         logger,
		Config:      config,
	}, nil
}

// PresignedGetURL returns a presigned URL from the s3 (or compatible) serice.
func (r *S3FileStorage) PresignedGetURL(scan executionv1.Scan, filename string, duration time.Duration) (string, error) {
	bucketName := r.Config.Bucket
	fileUrl := r.getPresignedUrlPath(scan, filename)
	reqParams := make(url.Values)
	rawResultDownloadURL, err := r.MinioClient.PresignedGetObject(context.Background(), bucketName, fileUrl, duration, reqParams)
	if err != nil {
		r.Log.Error(err, "Could not get presigned url from s3 or compatible storage provider")
		return "", err
	}
	return rawResultDownloadURL.String(), nil
}

// PresignedPutURL returns a presigned URL from the s3 (or compatible) serice.
func (r *S3FileStorage) PresignedPutURL(scan executionv1.Scan, filename string, duration time.Duration) (string, error) {
	bucketName := r.Config.Bucket
	fileUrl := r.getPresignedUrlPath(scan, filename)

	rawResultDownloadURL, err := r.MinioClient.PresignedPutObject(context.Background(), bucketName, fileUrl, duration)
	if err != nil {
		r.Log.Error(err, "Could not get presigned url from s3 or compatible storage provider")
		return "", err
	}
	return rawResultDownloadURL.String(), nil
}

// PresignedHeadURL returns a presigned URL from the s3 (or compatible) serice.
func (r *S3FileStorage) PresignedHeadURL(scan executionv1.Scan, filename string, duration time.Duration) (string, error) {
	bucketName := r.Config.Bucket
	fileUrl := r.getPresignedUrlPath(scan, filename)

	rawResultHeadURL, err := r.MinioClient.PresignedHeadObject(context.Background(), bucketName, fileUrl, duration, nil)
	if err != nil {
		r.Log.Error(err, "Could not get presigned url from s3 or compatible storage provider")
		return "", err
	}
	return rawResultHeadURL.String(), nil
}

func (r *S3FileStorage) DeleteFile(scan executionv1.Scan, filename string) error {
	bucketName := r.Config.Bucket
	pathInBucket := r.getPresignedUrlPath(scan, scan.Status.RawResultFile)
	err := r.MinioClient.RemoveObject(context.Background(), bucketName, pathInBucket, minio.RemoveObjectOptions{})
	if err != nil && err.Error() != "The specified key does not exist." {
		return err
	}
	return nil
}

func (r *S3FileStorage) getPresignedUrlPath(scan executionv1.Scan, filename string) string {
	urlTemplate := r.Config.UrlTemplate
	return executeUrlTemplate(urlTemplate, scan, filename)
}

func executeUrlTemplate(urlTemplate string, scan executionv1.Scan, filename string) string {
	type Template struct {
		Scan     executionv1.Scan
		Filename string
	}

	tmpl, err := template.New(urlTemplate).Parse(urlTemplate)
	if err != nil {
		panic(err)
	} else {
		var rawOutput bytes.Buffer
		templateArgs := Template{
			Scan:     scan,
			Filename: filename,
		}

		err = tmpl.Execute(&rawOutput, templateArgs)
		if err != nil {
			panic(err)
		}
		output := rawOutput.String()
		return output
	}
}
