package adminapi

import "strconv"

// APIVersion is the current major version of the adminapi wire contract.
// Clients may advertise the version they were built against via the
// X-Fabriq-Api-Version request header; the auth middleware rejects requests
// whose advertised major differs from this value (HTTP 426). Responses always
// carry the current APIVersion in the same header.
const APIVersion = 1

// apiVersionHeader is the request/response header carrying the adminapi major
// version. Requests may send it to opt into version checking; responses always
// carry it so clients can detect the server's contract version.
const apiVersionHeader = "X-Fabriq-Api-Version"

// apiVersionValue renders APIVersion as its header string form.
func apiVersionValue() string { return strconv.Itoa(APIVersion) }
