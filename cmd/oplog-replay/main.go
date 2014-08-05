package main

import (
	"bytes"
	"flag"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"

	"github.com/Clever/oplog-replay/replay"
)

func main() {
	speed := flag.Float64("speed", 1, "Sets multiplier for playback speed.")
	host := flag.String("host", "localhost", "Mongo host to playback onto.")
	s3path := flag.String("s3path", "", "S3 path to download data from instead of using stdin")
	retriesStr := flag.String("retries", "0", "Number of times to retry the replay")
	flag.Parse()

	var reader io.Reader
	reader = os.Stdin
	if len(*s3path) > 0 {
		temp_reader, err := getS3FileReader(*s3path)
		if err != nil {
			panic(err)
		}
		reader = temp_reader
	}

	retries, err := strconv.Atoi(*retriesStr)
	if err != nil {
		panic(err)
	}

	replayer := replay.New()
	if err := doWork(replayer, reader, *host, *speed, retries); err != nil {
		panic(err)
	}
}

// doWork attempts to replay the log. This includes retries. It's factored out to facilitate unit
// testing
func doWork(replayer replay.OplogReplayer, reader io.Reader, host string, speed float64, retries int) error {
	var err error
	for i := 0; i < retries+1; i++ {
		// If we succeeded in replaying the oplog then return no error
		if err = replayer.ReplayOplog(reader, speed, host); err == nil {
			return nil
		}
	}
	return err
}

func getS3FileReader(filepath string) (io.Reader, error) {
	auth, err := aws.EnvAuth()
	if err != nil {
		log.Fatal("AWS environment variables not set")
		return nil, err
	}

	s := s3.New(auth, aws.USWest)
	bucketName, s3path := parseS3Path(filepath)
	bucket := s.Bucket(bucketName)

	s3data, err := bucket.Get(s3path)
	if err != nil {
		log.Fatalf("Error downloading s3path: ", s3path)
		return nil, err
	}
	return bytes.NewReader(s3data), nil
}

// parseS3path parses an S3 path (s3://bucket/object) and returns a bucket, objectPath tuple
func parseS3Path(s3path string) (string, string) {
	// S3 path names are of the form s3://bucket/path
	stringsArray := strings.SplitAfterN(s3path, "/", 4)
	bucketName := stringsArray[2]
	bucketName = bucketName[0 : len(bucketName)-1]
	objectPath := stringsArray[3]
	return bucketName, objectPath
}
