package webdav

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/manager"
)

func TestParseDestinationPathHandlesAbsoluteURI(t *testing.T) {
	got, err := parseDestinationPath("https://dav.example:8443/webdav/__all__/Copied%20Folder")
	if err != nil {
		t.Fatalf("parseDestinationPath: %v", err)
	}
	if got != "/webdav/__all__/Copied Folder" {
		t.Fatalf("destination path = %q", got)
	}
}

func TestParseDestinationPathRejectsRelativeOrQualifiedPath(t *testing.T) {
	for _, destination := range []string{
		"__all__/relative",
		"https://dav.example/__all__/file?replace=true",
		"https://dav.example/__all__/file#fragment",
		"ftp://dav.example/__all__/file",
		"https://dav.example",
		"https://dav.example/__all__/folder%2Ffile",
	} {
		t.Run(destination, func(t *testing.T) {
			if _, err := parseDestinationPath(destination); err == nil {
				t.Fatalf("parseDestinationPath(%q) succeeded", destination)
			}
		})
	}
}

func TestHandleCopyRequestPassesDecodedDestinationAndOverwrite(t *testing.T) {
	request := httptest.NewRequest("COPY", "/__all__/Source", nil)
	request.Header.Set("Destination", "https://dav.example/webdav/__all__/Copied%20Folder")
	request.Header.Set("Overwrite", "F")
	response := httptest.NewRecorder()

	called := false
	handleCopyRequest(&manager.FileInfo{}, response, request, false, func(_ *manager.FileInfo, destination string, move, overwrite bool) (bool, error) {
		called = true
		if destination != "/webdav/__all__/Copied Folder" || move || overwrite {
			t.Fatalf("copy args = destination:%q move:%v overwrite:%v", destination, move, overwrite)
		}
		return true, nil
	})
	if !called {
		t.Fatal("copy function was not called")
	}
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", response.Code)
	}
}

func TestHandleCopyRequestReturnsCreatedOrNoContent(t *testing.T) {
	for _, test := range []struct {
		name    string
		created bool
		move    bool
		want    int
	}{
		{name: "new copy", created: true, want: http.StatusCreated},
		{name: "replacement copy", created: false, want: http.StatusNoContent},
		{name: "new move", created: true, move: true, want: http.StatusCreated},
		{name: "replacement move", created: false, move: true, want: http.StatusNoContent},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest("COPY", "/__all__/Source", nil)
			request.Header.Set("Destination", "/__all__/Destination")
			response := httptest.NewRecorder()
			handleCopyRequest(&manager.FileInfo{}, response, request, test.move, func(_ *manager.FileInfo, _ string, move, overwrite bool) (bool, error) {
				if move != test.move || !overwrite {
					t.Fatalf("move/overwrite = %v/%v", move, overwrite)
				}
				return test.created, nil
			})
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d", response.Code, test.want)
			}
		})
	}
}

func TestHandleCopyRequestMapsOverwriteConflictTo412(t *testing.T) {
	request := httptest.NewRequest("COPY", "/__all__/Source", nil)
	request.Header.Set("Destination", "/__all__/Destination")
	request.Header.Set("Overwrite", "F")
	response := httptest.NewRecorder()

	handleCopyRequest(&manager.FileInfo{}, response, request, false, func(_ *manager.FileInfo, _ string, _ bool, overwrite bool) (bool, error) {
		if overwrite {
			t.Fatal("Overwrite:F was passed as true")
		}
		return false, manager.ErrCopyDestinationExists
	})
	if response.Code != http.StatusPreconditionFailed {
		t.Fatalf("status = %d, want 412", response.Code)
	}
}

func TestHandleCopyRequestRejectsBadHeadersBeforeMutation(t *testing.T) {
	for _, test := range []struct {
		name        string
		destination string
		overwrite   string
	}{
		{name: "missing destination"},
		{name: "bad destination", destination: "relative/path"},
		{name: "bad overwrite", destination: "/__all__/Destination", overwrite: "sometimes"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest("COPY", "/__all__/Source", nil)
			request.Header.Set("Destination", test.destination)
			request.Header.Set("Overwrite", test.overwrite)
			response := httptest.NewRecorder()
			called := false
			handleCopyRequest(&manager.FileInfo{}, response, request, false, func(_ *manager.FileInfo, _ string, _ bool, _ bool) (bool, error) {
				called = true
				return true, nil
			})
			if called {
				t.Fatal("copy function called for invalid headers")
			}
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", response.Code)
			}
		})
	}
}

func TestHandleCopyRequestMapsUnsupportedDestinationTo409(t *testing.T) {
	request := httptest.NewRequest("MOVE", "/__all__/Source/file", nil)
	request.Header.Set("Destination", "/__all__/Other/file")
	response := httptest.NewRecorder()
	handleCopyRequest(&manager.FileInfo{}, response, request, true, func(_ *manager.FileInfo, _ string, _ bool, _ bool) (bool, error) {
		return false, errors.Join(errors.New("copy failed"), manager.ErrCopyUnsupported)
	})
	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", response.Code)
	}
}
