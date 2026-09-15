// Package s3 is the request model: bucket and key extraction for virtual-host and path style,
// the operation classifier (a table, not an if-ladder), and S3 error rendering
// (docs/DESIGN.md §1.2). errors.go holds shunt's own table of S3 error codes, HTTP statuses, and
// AWS messages with the XML renderer; s3response/ holds the response XML structs lifted from
// versitygw. The request model and classifier are built in POC-1.
package s3
