package client

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
)

// SearchPage is the payload for GET {BasePath}/search. It mirrors adminapi's
// searchResponse JSON exactly: {items}. Each item mirrors EntityRecord
// ({id, type, data}).
type SearchPage struct {
	Items []EntityRecord `json:"items"`
}

// SearchTextParams are the query parameters for SearchText. Type and Query
// are required by the backend; Limit, Offset, Sort and Filter are optional.
type SearchTextParams struct {
	// Type is the registered dynamic entity type name (e.g. "product"). Must
	// be search-indexed.
	Type string
	// Query is the full-text query string.
	Query string
	// Limit caps the page size (server default 25, capped server-side). Zero
	// omits the query param and defers to the server default.
	Limit int
	// Offset paginates past earlier results. Zero omits the query param.
	Offset int
	// Sort is an indexed column, optionally suffixed " DESC". Empty omits
	// the query param and defers to relevance-score ordering.
	Sort string
	// Filter is a set of equality filters over indexed fields, AND-ed. Each
	// entry is sent as a repeated ?filter=field:value query param.
	Filter map[string]string
}

// SearchText performs full-text search over an entity's indexed fields. It
// calls GET {BasePath}/search?type=<type>&q=<query>[&limit=<n>][&offset=<n>]
// [&sort=<s>][&filter=field:value ...].
func (c *Client) SearchText(ctx context.Context, params SearchTextParams) (SearchPage, error) {
	q := url.Values{}
	q.Set("type", params.Type)
	q.Set("q", params.Query)
	if params.Limit > 0 {
		q.Set("limit", strconv.Itoa(params.Limit))
	}
	if params.Offset > 0 {
		q.Set("offset", strconv.Itoa(params.Offset))
	}
	if params.Sort != "" {
		q.Set("sort", params.Sort)
	}
	for field, val := range params.Filter {
		if field == "" || val == "" {
			continue
		}
		q.Add("filter", field+":"+val)
	}

	var out SearchPage
	if err := c.do(ctx, http.MethodGet, "/search", q, nil, &out); err != nil {
		return SearchPage{}, err
	}
	return out, nil
}
