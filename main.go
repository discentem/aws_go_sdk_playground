package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"log"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func getConfig(bucketAddress string) (*aws.Config, error) {
	var cfg aws.Config
	var err error

	if strings.HasPrefix(bucketAddress, "s3://") {
		cfg, err = config.LoadDefaultConfig(context.TODO())
		if err != nil {
			return nil, err
		}
		return &cfg, nil
	} else if strings.HasPrefix(bucketAddress, "https://") || strings.HasPrefix(bucketAddress, "http://") {
		// e.g. http://127.0.0.1:9000/test becomes http://127.0.0.1:9000
		server, _ := path.Split(bucketAddress)
		// https://stackoverflow.com/questions/67575681/is-aws-go-sdk-v2-integrated-with-local-minio-server
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...any) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:       "aws",
				URL:               server,
				SigningRegion:     "us-east-1",
				HostnameImmutable: true,
			}, nil
		})

		cfg, err := config.LoadDefaultConfig(context.Background(),
			config.WithRegion("us-east-1"),
			config.WithEndpointResolverWithOptions(resolver),
		)
		if err != nil {
			return nil, err
		}
		return &cfg, nil
	}
	return nil, errors.New("bucketAddress did not contain s3://, http://, or https:// prefix")
}

func createBucket(ctx context.Context, bucketAddress string) error {
	cfg, err := getConfig(bucketAddress)
	if err != nil {
		return err
	}
	client := s3.NewFromConfig(*cfg)
	_, bucket := path.Split(bucketAddress)
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		return err
	}
	return nil
}

func upload(ctx context.Context, bucketAddress, key string, b []byte) error {
	cfg, err := getConfig(bucketAddress)
	if err != nil {
		return err
	}
	uploader := s3manager.NewUploader(s3.NewFromConfig(*cfg))
	uploader.Concurrency = 3

	_, bucket := path.Split(bucketAddress)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(b),
	})
	if err != nil {
		log.Print("uploader.Upload returned an error")
		return err
	}
	return nil
}

func listObjects(bucketAddress string) error {
	cfg, err := getConfig(bucketAddress)
	if err != nil {
		return err
	}
	client := s3.NewFromConfig(*cfg)
	_, bucket := path.Split(bucketAddress)
	listObjsResponse, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(""),
	})

	if err != nil {
		return err
	}

	for _, object := range listObjsResponse.Contents {
		log.Printf("%s (%d bytes, class %v) \n", *object.Key, object.Size, object.StorageClass)
	}
	return nil
}

func main() {
	b := []byte("blah")
	ctx := context.Background()

	bucketAddress := flag.String("bucket_address", "http://127.0.0.1:9000/test", "address to bucket")
	createBuck := flag.Bool("create_bucket", false, "skip bucket creation if false")
	if *createBuck {
		if err := createBucket(ctx, *bucketAddress); err != nil {
			log.Print(err)
		}
	}
	if err := upload(ctx, *bucketAddress, "thing", b); err != nil {
		log.Fatal(err)
	}
	if err := listObjects(*bucketAddress); err != nil {
		log.Fatal(err)
	}

}
