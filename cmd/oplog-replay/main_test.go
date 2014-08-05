package main

import (
	"errors"
	"io"
	"os"
	"testing"
)

func TestParseS3Path(t *testing.T) {
	bucketName, s3path := parseS3Path("s3://clever-files/directory/path")
	if bucketName != "clever-files" {
		t.Fatal("Bucket name is " + bucketName + ", not clever-files")
	}
	if s3path != "directory/path" {
		t.Fatal("Path is " + s3path + ", not directory/path")
	}
}

type MockReplayer struct {
	timesToFail int
	timesCalled int
}

func (mock *MockReplayer) ReplayOplog(reader io.Reader, speed float64, host string) error {
	mock.timesCalled++
	if mock.timesToFail == 0 {
		return nil
	} else {
		mock.timesToFail--
		return errors.New("Failing!")
	}
}

func TestSuccessWithNoRetries(t *testing.T) {
	// If the replayer works the first time we shouldn't need replays
	mock := &MockReplayer{timesToFail: 0}
	if err := doWork(mock, os.Stdin, "localhost", 1, 0); err != nil {
		t.Fatal("Should have failed")
	}
	if mock.timesCalled != 1 {
		t.Fatal("Mock replayer was never called")
	}
}

func TestSucceedsAfterEnoughRetries(t *testing.T) {
	// With only one retry this should fail
	mock := &MockReplayer{timesToFail: 2}
	if err := doWork(mock, os.Stdin, "localhost", 1, 1); err == nil {
		t.Fatal("Should have failed with only on retry")
	}

	// With two it should succeed
	mock = &MockReplayer{timesToFail: 2}
	if err := doWork(mock, os.Stdin, "localhost", 1, 2); err != nil {
		t.Fatal("Shouldn't have failed with retries")
	}
}
