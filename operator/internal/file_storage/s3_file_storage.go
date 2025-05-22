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

type S3FileStorage struct {
	MinioClient *minio.Client
	Log         logr.Logger
}

func NewS3FileStorage(logger logr.Logger) (*S3FileStorage, error) {
	endpoint := os.Getenv("S3_ENDPOINT")
	if os.Getenv("S3_PORT") != "" {
		endpoint = fmt.Sprintf("%s:%s", endpoint, os.Getenv("S3_PORT"))
	}
	// Only deactivate useSSL when explicitly set to false
	useSSL := true
	if os.Getenv("S3_USE_SSL") == "false" {
		useSSL = false
	}

	var creds *credentials.Credentials

	if authType, ok := os.LookupEnv("S3_AUTH_TYPE"); ok && strings.ToLower(authType) == "aws-irsa" {
		stsEndpoint := ""
		if configuredStsEndpoint, ok := os.LookupEnv("S3_AWS_IRSA_STS_ENDPOINT"); ok {
			stsEndpoint = configuredStsEndpoint
		}

		logger.Info("Using AWS IRSA ServiceAccount Bindung for S3 Authentication", "sts", stsEndpoint)
		creds = credentials.NewIAM(stsEndpoint)
	} else {
		creds = credentials.NewEnvMinio()
	}

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  creds,
		Secure: useSSL,
	})
	if err != nil {
		logger.Error(err, "Could not create minio client to communicate with s3 or compatible storage provider")
		return nil, err
	}

	return &S3FileStorage{
		MinioClient: minioClient,
	}, nil
}

// PresignedGetURL returns a presigned URL from the s3 (or compatible) serice.
func (r *S3FileStorage) PresignedGetURL(scan executionv1.Scan, filename string, duration time.Duration) (string, error) {
	bucketName := os.Getenv("S3_BUCKET")

	fileUrl := getPresignedUrlPath(scan, filename)
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
	bucketName := os.Getenv("S3_BUCKET")
	fileUrl := getPresignedUrlPath(scan, filename)

	rawResultDownloadURL, err := r.MinioClient.PresignedPutObject(context.Background(), bucketName, fileUrl, duration)
	if err != nil {
		r.Log.Error(err, "Could not get presigned url from s3 or compatible storage provider")
		return "", err
	}
	return rawResultDownloadURL.String(), nil
}

// PresignedHeadURL returns a presigned URL from the s3 (or compatible) serice.
func (r *S3FileStorage) PresignedHeadURL(scan executionv1.Scan, filename string, duration time.Duration) (string, error) {
	bucketName := os.Getenv("S3_BUCKET")
	fileUrl := getPresignedUrlPath(scan, filename)

	rawResultHeadURL, err := r.MinioClient.PresignedHeadObject(context.Background(), bucketName, fileUrl, duration, nil)
	if err != nil {
		r.Log.Error(err, "Could not get presigned url from s3 or compatible storage provider")
		return "", err
	}
	return rawResultHeadURL.String(), nil
}

func (r *S3FileStorage) DeleteFile(scan executionv1.Scan, filename string) error {
	bucketName := os.Getenv("S3_BUCKET")
	pathInBucket := getPresignedUrlPath(scan, scan.Status.RawResultFile)
	err := r.MinioClient.RemoveObject(context.Background(), bucketName, pathInBucket, minio.RemoveObjectOptions{})
	if err != nil && err.Error() != "The specified key does not exist." {
		return err
	}
	return nil
}

func getPresignedUrlPath(scan executionv1.Scan, filename string) string {
	urlTemplate, ok := os.LookupEnv("S3_URL_TEMPLATE")
	if !ok {
		// use default when environment variable is not set
		urlTemplate = "scan-{{ .Scan.UID }}/{{ .Filename }}"
	}
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
