package wal

import (
	"errors"
	"testing"
)

func TestProperty_WALChecksumChain(t *testing.T) {
	for _, bigEndian := range []bool{false, true} {
		pageSize := uint32(4096)
		header := BuildHeader(pageSize, 0x11223344, 0x55667788, bigEndian)
		parsedHeader, err := ParseHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if parsedHeader.BigEndianChecksum != bigEndian {
			t.Fatalf("byte order not preserved: got %v want %v", parsedHeader.BigEndianChecksum, bigEndian)
		}
		sum, err := InitialChecksum(header, bigEndian)
		if err != nil {
			t.Fatal(err)
		}
		rawFrames := make([][]byte, 0, 3)
		for index := 0; index < 3; index++ {
			page := make([]byte, pageSize)
			for offset := range page {
				page[offset] = byte(index + 1)
			}
			databaseSize := uint32(0)
			if index == 2 {
				databaseSize = 4
			}
			raw, err := BuildFrame(uint32(index+1), databaseSize, parsedHeader.Salt1, parsedHeader.Salt2, page, parsedHeader, sum)
			if err != nil {
				t.Fatal(err)
			}
			rawFrames = append(rawFrames, raw)
			sum, err = FrameChecksum(raw, pageSize, sum, bigEndian)
			if err != nil {
				t.Fatal(err)
			}
		}

		chain, err := VerifyChain(header, rawFrames)
		if err != nil {
			t.Fatalf("valid chain rejected: %v", err)
		}
		if len(chain.Frames) != 3 {
			t.Fatalf("frame count: got %d want 3", len(chain.Frames))
		}
		if chain.MXFrame != 3 {
			t.Fatalf("mxFrame: got %d want 3", chain.MXFrame)
		}
		if LastCommitIndex(chain.Frames) != 3 {
			t.Fatal("commit marker not detected on final frame")
		}

		corrupt := make([][]byte, len(rawFrames))
		copy(corrupt, rawFrames)
		corrupt[1] = append([]byte(nil), rawFrames[1]...)
		corrupt[1][FrameHeaderSize+10] ^= 0xff
		if _, err := VerifyChain(header, corrupt); !errors.Is(err, ErrChecksum) {
			t.Fatalf("corrupt page: got %v want %v", err, ErrChecksum)
		}

		badSalt := make([][]byte, len(rawFrames))
		copy(badSalt, rawFrames)
		badSalt[0] = append([]byte(nil), rawFrames[0]...)
		badSalt[0][8] ^= 0xff
		if _, err := VerifyChain(header, badSalt); !errors.Is(err, ErrSaltMismatch) {
			t.Fatalf("salt mismatch: got %v want %v", err, ErrSaltMismatch)
		}

		commits := []Frame{{DatabaseSize: 0}, {DatabaseSize: 5}, {DatabaseSize: 0}}
		if got := LastCommitIndex(commits); got != 2 {
			t.Fatalf("last commit: got %d want 2", got)
		}
	}
}
