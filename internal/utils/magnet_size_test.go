package utils

import (
	"bytes"
	"testing"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
)

// A MULTI-FILE TORRENT HAS A SIZE, AND IT IS NOT ZERO.
//
// metainfo.Info.Length is populated ONLY for single-file torrents; a multi-file
// torrent leaves it at zero and carries its sizes in Info.Files. Reading Length
// therefore recorded every season pack as 0 bytes, and the *arr rows showed 0
// bytes to match.
//
// It reached whole indexers at a time rather than occasional entries, because
// this is the only torrent-FILE path: an indexer that hands the *arr a Prowlarr
// /download URL instead of a magnet resolves through OpenMagnetHttpURL into
// GetMagnetFromBytes. A magnet-supplying indexer never touches it.
func buildTorrent(t *testing.T, info metainfo.Info) []byte {
	t.Helper()
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatalf("marshal info: %v", err)
	}
	var buf bytes.Buffer
	if err := bencode.NewEncoder(&buf).Encode(metainfo.MetaInfo{
		InfoBytes: infoBytes,
	}); err != nil {
		t.Fatalf("encode metainfo: %v", err)
	}
	return buf.Bytes()
}

func TestGetMagnetFromBytesSizesMultiFileTorrents(t *testing.T) {
	const pieceLength = 256 * 1024

	multi := buildTorrent(t, metainfo.Info{
		Name:        "Some.Show.S01.1080p",
		PieceLength: pieceLength,
		Pieces:      make([]byte, 20),
		Files: []metainfo.FileInfo{
			{Path: []string{"S01E01.mkv"}, Length: 1_500_000_000},
			{Path: []string{"S01E02.mkv"}, Length: 1_400_000_000},
			{Path: []string{"S01E03.mkv"}, Length: 1_600_000_000},
		},
	})

	magnet, err := GetMagnetFromBytes(multi, false)
	if err != nil {
		t.Fatalf("parse multi-file torrent: %v", err)
	}

	const want = int64(4_500_000_000)
	if magnet.Size != want {
		t.Fatalf("multi-file torrent size = %d, want %d. A season pack recorded as %d bytes shows the *arr a "+
			"0-byte row and gives every size-based decision nothing to work with", magnet.Size, want, magnet.Size)
	}
}

// The single-file shape must keep reporting exactly what it always did —
// TotalLength returns Length there, so this is the regression guard on the
// change rather than new behaviour.
func TestGetMagnetFromBytesKeepsSingleFileSize(t *testing.T) {
	const size = int64(2_100_000_000)

	single := buildTorrent(t, metainfo.Info{
		Name:        "Some.Movie.2023.2160p.mkv",
		Length:      size,
		PieceLength: 256 * 1024,
		Pieces:      make([]byte, 20),
	})

	magnet, err := GetMagnetFromBytes(single, false)
	if err != nil {
		t.Fatalf("parse single-file torrent: %v", err)
	}
	if magnet.Size != size {
		t.Fatalf("single-file torrent size = %d, want %d", magnet.Size, size)
	}
	if magnet.Name != "Some.Movie.2023.2160p.mkv" {
		t.Fatalf("name = %q", magnet.Name)
	}
}
