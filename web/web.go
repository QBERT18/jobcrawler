// Package web holds the static single-page job-browser frontend, embedded into
// the api binary so it ships inside the scratch image with no extra files.
package web

import _ "embed"

// IndexHTML is the served single-page job browser (web/index.html).
//
//go:embed index.html
var IndexHTML []byte
