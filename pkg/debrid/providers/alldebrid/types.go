package alldebrid

import (
	"bytes"
	"fmt"

	json "github.com/bytedance/sonic"
)

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type apiResponse struct {
	Status string         `json:"status"`
	Error  *errorResponse `json:"error"`
}

type MagnetFile struct {
	Name     string       `json:"n"`
	Size     int64        `json:"s"`
	Link     string       `json:"l"`
	Elements []MagnetFile `json:"e"`
}
type magnetInfo struct {
	Id             int          `json:"id"`
	Filename       string       `json:"filename"`
	Size           int64        `json:"size"`
	Hash           string       `json:"hash"`
	Status         string       `json:"status"`
	StatusCode     int          `json:"statusCode"`
	UploadDate     int64        `json:"uploadDate"`
	Downloaded     int64        `json:"downloaded"`
	Uploaded       int64        `json:"uploaded"`
	DownloadSpeed  int64        `json:"downloadSpeed"`
	UploadSpeed    int64        `json:"uploadSpeed"`
	Seeders        int          `json:"seeders"`
	CompletionDate int64        `json:"completionDate"`
	Type           string       `json:"type"`
	Notified       bool         `json:"notified"`
	Version        int          `json:"version"`
	NbLinks        int          `json:"nbLinks"`
	Files          []MagnetFile `json:"files"`
}

// Magnets tolerates the shapes AllDebrid uses for data.magnets: a JSON array
// (list and v4.1 per-id responses), a single JSON object (v4-style per-id
// responses), or a map keyed by magnet ID.
type Magnets []magnetInfo

// UnmarshalJSON implements custom unmarshaling for the Magnets type.
// If the input is an array, it is unmarshaled directly into the slice.
// If the input is a single magnet object, it is wrapped as a one-element slice.
// If the input is a map keyed by magnet ID, its values are collected.
// If the input is none of these, it returns an error.
func (m *Magnets) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		*m = nil
		return nil
	}

	if data[0] == '[' {
		var arr []magnetInfo
		if err := json.Unmarshal(data, &arr); err != nil {
			return fmt.Errorf("magnets: decoding array: %w", err)
		}
		*m = arr
		return nil
	}

	// A single magnet object (v4-style per-id responses). A map keyed by
	// magnet ID also decodes into magnetInfo (unknown keys are ignored), so
	// require a non-zero ID before accepting the single-object shape.
	var single magnetInfo
	if err := json.Unmarshal(data, &single); err == nil && single.Id != 0 {
		*m = Magnets{single}
		return nil
	}

	// A map keyed by magnet ID.
	var byID map[string]magnetInfo
	if err := json.Unmarshal(data, &byID); err == nil {
		magnets := make(Magnets, 0, len(byID))
		for _, v := range byID {
			magnets = append(magnets, v)
		}
		*m = magnets
		return nil
	}

	return fmt.Errorf("magnets: unsupported JSON format")
}

type MagnetStatusResponse struct {
	Status string `json:"status"`
	Data   struct {
		Magnets Magnets `json:"magnets"`
	} `json:"data"`
	Error *errorResponse `json:"error"`
}

type UploadMagnetResponse struct {
	Status string `json:"status"`
	Data   struct {
		Magnets []struct {
			Magnet           string `json:"magnet"`
			Hash             string `json:"hash"`
			Name             string `json:"name"`
			FilenameOriginal string `json:"filename_original"`
			Size             int64  `json:"size"`
			Ready            bool   `json:"ready"`
			ID               int    `json:"id"`
		} `json:"magnets"`
	}
	Error *errorResponse `json:"error"`
}

type UploadFileResponse struct {
	Status string `json:"status"`
	Data   struct {
		Files []struct {
			File  string         `json:"file"`
			Name  string         `json:"name"`
			Hash  string         `json:"hash"`
			ID    int            `json:"id"`
			Size  int64          `json:"size"`
			Ready bool           `json:"ready"`
			Error *errorResponse `json:"error"`
		} `json:"files"`
	} `json:"data"`
	Error *errorResponse `json:"error"`
}

type DownloadLink struct {
	Status string `json:"status"`
	Data   struct {
		Link      string `json:"link"`
		Host      string `json:"host"`
		Filename  string `json:"filename"`
		Streaming []any  `json:"streaming"`
		Paws      bool   `json:"paws"`
		Filesize  int    `json:"filesize"`
		Id        string `json:"id"`
		Path      []struct {
			Name string `json:"n"`
			Size int    `json:"s"`
		} `json:"path"`
	} `json:"data"`
	Error *errorResponse `json:"error"`
}

type LinkInfosResponse struct {
	Status string `json:"status"`
	Data   struct {
		Infos []struct {
			Error *errorResponse `json:"error"`
		} `json:"infos"`
	} `json:"data"`
	Error *errorResponse `json:"error"`
}

type UserProfileResponse struct {
	Status string         `json:"status"`
	Error  *errorResponse `json:"error"`
	Data   struct {
		User struct {
			Username             string         `json:"username"`
			Email                string         `json:"email"`
			IsPremium            bool           `json:"isPremium"`
			IsSubscribed         bool           `json:"isSubscribed"`
			IsTrial              bool           `json:"isTrial"`
			PremiumUntil         int64          `json:"premiumUntil"`
			Lang                 string         `json:"lang"`
			FidelityPoints       int            `json:"fidelityPoints"`
			LimitedHostersQuotas map[string]int `json:"limitedHostersQuotas"`
			Notifications        []string       `json:"notifications"`
		} `json:"user"`
	} `json:"data"`
}

type LinksListResponse struct {
	Status string `json:"status"`
	Data   struct {
		Links []struct {
			Link     string `json:"link"`
			Filename string `json:"filename"`
			Size     int64  `json:"size"`
			Host     string `json:"host"`
			Date     int64  `json:"date"`
		} `json:"links"`
	} `json:"data"`
	Error *errorResponse `json:"error"`
}
