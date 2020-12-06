package caddys3proxy

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"
	"html/template"
	"io"
	"net/http"
	"path"
	"reflect"
	"strings"
	"time"
)

func init() {
	caddy.RegisterModule(S3Proxy{})
}

// S3Proxy implements a proxy to return, set, delete or browse objects from S3
type S3Proxy struct {
	// The path to the root of the site. Default is `{http.vars.root}` if set,
	// Or if not set the value is "" - meaning use the whole path as a key.
	Root string `json:"root,omitempty"`

	// The AWS region the bucket is hosted in
	Region string `json:"region,omitempty"`

	// The name of the S3 bucket
	Bucket string `json:"bucket,omitempty"`

	// Use non-standard endpoint for S3
	Endpoint string `json:"endpoint,omitempty"`

	// Set this to `true` to enable S3 Accelerate feature.
	S3UseAccelerate bool `json:"use_accelerate,omitempty"`

	client      *s3.S3
	dirTemplate *template.Template
	log         *zap.Logger
}

// CaddyModule returns the Caddy module information.
func (S3Proxy) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.s3proxy",
		New: func() caddy.Module { return new(S3Proxy) },
	}
}

func (p *S3Proxy) Provision(ctx caddy.Context) (err error) {
	p.log = ctx.Logger(p)

	if p.Root == "" {
		p.Root = "{http.vars.root}"
	}

	var config aws.Config

	// If Region is not specified NewSession will look for it from an env value AWS_REGION
	if p.Region != "" {
		config.Region = aws.String(p.Region)
	}

	if p.Endpoint != "" {
		config.Endpoint = aws.String(p.Endpoint)
	}

	if p.S3UseAccelerate {
		config.S3UseAccelerate = aws.Bool(p.S3UseAccelerate)
	}

	sess, err := session.NewSession(&config)
	if err != nil {
		p.log.Error("could not create AWS session",
			zap.String("error", err.Error()),
		)
		return err
	}

	// Create S3 service client
	p.client = s3.New(sess)
	p.log.Info("S3 proxy initialized for bucket: " + p.Bucket)
	p.log.Debug("config values",
		zap.String("endpoint", p.Endpoint),
		zap.String("region", p.Region),
		zap.Bool("use_accelerate", p.S3UseAccelerate),
	)

	return nil
}

func (p S3Proxy) getS3Object(bucket string, key string, r *http.Request, w http.ResponseWriter) (*s3.GetObjectOutput, error) {
	oi := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	headers := r.Header
	if rg := headers.Get("Range"); rg != "" {
		oi = oi.SetRange(rg)
	}
	if ifMatch := headers.Get("If-Match"); ifMatch != "" {
		oi = oi.SetIfMatch(ifMatch)
	}
	if ifNoneMatch := headers.Get("If-None-Match"); ifNoneMatch != "" {
		oi = oi.SetIfNoneMatch(ifNoneMatch)
	}
	if ifModifiedSince := headers.Get("If-Modified-Since"); ifModifiedSince != "" {
		t, err := time.Parse(http.TimeFormat, ifModifiedSince)
		if err == nil {
			oi = oi.SetIfModifiedSince(t)
		}
	}
	if ifUnmodifiedSince := headers.Get("If-Unmodified-Since"); ifUnmodifiedSince != "" {
		t, err := time.Parse(http.TimeFormat, ifUnmodifiedSince)
		if err == nil {
			oi = oi.SetIfUnmodifiedSince(t)
		}
	}

	p.log.Debug("cache:attempt",
		zap.String("bucket", bucket),
		zap.String("key", key),
	)

	obj, err := p.client.GetObject(oi)

	if err != nil {
		// Make the err a caddyErr if it is not already
		awsErr, isAwsErr := err.(awserr.Error)

		if isAwsErr {
			switch awsErr.Code() {
			case "NotModified":
				p.log.Debug("cache:hit",
					zap.String("bucket", bucket),
					zap.String("key", key),
					zap.String("code", awsErr.Code()),
				)
				w.WriteHeader(304)
				return obj, nil
			case "NoSuchKey":
				p.log.Debug("cache:miss",
					zap.String("bucket", bucket),
					zap.String("key", key),
					zap.String("code", awsErr.Code()),
					zap.String("error", awsErr.Error()),
				)
			default:
				p.log.Error("cache:fail",
					zap.String("bucket", bucket),
					zap.String("key", key),
					zap.String("code", awsErr.Code()),
					zap.String("error", awsErr.Error()),
				)
			}

		} else {
			p.log.Error("cache:fail",
				zap.String("bucket", bucket),
				zap.String("key", key),
				zap.String("error", err.Error()),
			)
		}

		return obj, err
	}

	if *obj.ContentLength == 0 {
		p.log.Error("cache:fail",
			zap.String("bucket", bucket),
			zap.String("key", key),
			zap.String("error", "ContentLength is empty"),
		)

		return obj, errors.New("ContentLength is empty - cache is ")
	}

	p.log.Debug("cache:hit",
		zap.String("bucket", bucket),
		zap.String("key", key),
	)

	return obj, nil
}

func joinPath(root string, uriPath string) string {
	isDir := uriPath[len(uriPath)-1:] == "/"
	newPath := path.Join(root, uriPath)
	if isDir && newPath != "/" {
		// Join will strip the ending /
		// add it back if it was there as it implies a dir view
		return newPath + "/"
	}
	return newPath
}

func (p S3Proxy) writeResponseFromGetObject(w http.ResponseWriter, obj *s3.GetObjectOutput) error {
	// Copy headers from AWS response to our response
	setStrHeader(w, "Cache-Control", obj.CacheControl)
	setStrHeader(w, "Content-Disposition", obj.ContentDisposition)
	setStrHeader(w, "Content-Encoding", obj.ContentEncoding)
	setStrHeader(w, "Content-Language", obj.ContentLanguage)
	setStrHeader(w, "Content-Range", obj.ContentRange)
	setStrHeader(w, "Content-Type", obj.ContentType)
	setStrHeader(w, "ETag", obj.ETag)
	setStrHeader(w, "Expires", obj.Expires)
	setTimeHeader(w, "Last-Modified", obj.LastModified)

	// Adds all custom headers which where used on this object
	for key, value := range obj.Metadata {
		setStrHeader(w, key, value)
	}

	w.Header().Set("X-Cache-S3", "hit")

	var err error
	if obj.Body != nil {
		// io.Copy will set Content-Length
		w.Header().Del("Content-Length")
		_, err = io.Copy(w, obj.Body)
	}

	return err
}

// ServeHTTP implements the main entry point for a request for the caddyhttp.Handler interface.
func (p S3Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	p.log.Debug("incoming request",
		zap.String("r.method", r.Method),
		zap.String("r.URL.path", r.URL.Path),
		zap.String("r.URL.RawQuery", r.URL.RawQuery),
	)

	if r.Method != http.MethodGet { // As of now only support GET requests
		p.log.Debug("cache:miss",
			zap.String("r.method", r.Method),
			zap.String("r.URL.path", r.URL.Path),
			zap.String("r.URL.RawQuery", r.URL.RawQuery),
			zap.String("message", "method not allowed"),
		)

		return next.ServeHTTP(w, r)
	}

	repl := r.Context().Value(caddy.ReplacerCtxKey).(*caddy.Replacer)
	fullPath := joinPath(repl.ReplaceAll(p.Root, ""), r.URL.Path)

	var err error

	err = p.GetHandler(w, r, fullPath)

	if err == nil {
		return nil
	}

	return next.ServeHTTP(w, r)
}

func (p S3Proxy) GetHandler(w http.ResponseWriter, r *http.Request, fullPath string) error {
	var obj *s3.GetObjectOutput
	var err error
	var defaultIndex = "index.html"
	var s3Key = fullPath

	if strings.HasSuffix(fullPath, "/") { // If we have a trailing-slash, then use the defaultIndex
		s3Key = path.Join(s3Key, defaultIndex)
	}

	if len(r.URL.RawQuery) > 0 { // RawQuery is converted to sha1() and put in a subdirectory
		s3Key = path.Join(s3Key, "/", convertSha1(r.URL.RawQuery))
	}

	obj, err = p.getS3Object(p.Bucket, s3Key, r, w)
	if err != nil {
		return err
	}

	return p.writeResponseFromGetObject(w, obj)
}

func convertSha1(in string) string {
	h := sha1.New()
	h.Write([]byte(in))
	return hex.EncodeToString(h.Sum(nil))
}

func setStrHeader(w http.ResponseWriter, key string, value *string) {
	if value != nil && len(*value) > 0 {
		w.Header().Set(key, *value)
	}
}

func setTimeHeader(w http.ResponseWriter, key string, value *time.Time) {
	if value != nil && !reflect.DeepEqual(*value, time.Time{}) {
		w.Header().Set(key, value.UTC().Format(http.TimeFormat))
	}
}
