package cdc

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3ListClient is the one-method surface a prefix listing needs. *s3.Client
// satisfies it, as does every wider client interface that carries
// ListObjectsV2 (S3InitClient, reconcile's object API, the e2e harness's
// cluster client).
type S3ListClient interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// ErrIncompleteObjectListing marks a listing whose pages cannot be followed
// to the end: a truncated page without a continuation token, or a token the
// listing has already used. The inventory built on such a listing would be
// short — cdc-init's --replace-delta purge would delete less than the delta
// tier holds (#371 review), and manifest-reconcile would under-report
// orphans and over-report dangling entries while still saying "ok" (#521) —
// so the listing fails closed instead.
var ErrIncompleteObjectListing = errors.New("object listing incomplete")

// ForEachObject is the shared ListObjectsV2 paginator: it calls fn for every
// object under prefix, following continuation tokens until the listing is
// exhausted. A page that claims to be truncated but carries no usable
// token, or hands back a token already used, is not delivered to fn; the
// listing fails with ErrIncompleteObjectListing rather than presenting the
// objects seen so far as the whole prefix. An error from fn stops the
// listing at that page and is returned unwrapped, so callers keep the
// context they attached themselves.
//
// Every production and harness paginator routes through here so the
// fail-closed rule has exactly one copy (#521).
func ForEachObject(ctx context.Context, client S3ListClient, bucket, prefix string, fn func(types.Object) error) error {
	var token *string
	seen := map[string]struct{}{}
	for {
		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return fmt.Errorf("list objects under %s: %w", prefix, err)
		}
		next, err := nextListingToken(out, prefix, len(seen)+1, seen)
		if err != nil {
			return err
		}
		for _, obj := range out.Contents {
			if err := fn(obj); err != nil {
				return err
			}
		}
		if next == nil {
			return nil
		}
		token = next
	}
}

// nextListingToken validates one page's pagination state and returns the
// token to forward, or nil when the page is the last one. It records the
// token in seen so a later page that hands it back again is refused.
func nextListingToken(out *s3.ListObjectsV2Output, prefix string, page int, seen map[string]struct{}) (*string, error) {
	if !aws.ToBool(out.IsTruncated) {
		return nil, nil
	}
	next := aws.ToString(out.NextContinuationToken)
	if next == "" {
		return nil, fmt.Errorf("list objects under %s: page %d is truncated but carries no continuation token: %w",
			prefix, page, ErrIncompleteObjectListing)
	}
	if _, repeated := seen[next]; repeated {
		return nil, fmt.Errorf("list objects under %s: page %d repeats continuation token %q: %w",
			prefix, page, next, ErrIncompleteObjectListing)
	}
	seen[next] = struct{}{}
	return aws.String(next), nil
}

// ListObjectKeys returns every object key under prefix, following
// continuation tokens through ForEachObject and inheriting its fail-closed
// rule: an unfollowable page fails with ErrIncompleteObjectListing rather
// than returning the keys seen so far.
func ListObjectKeys(ctx context.Context, client S3ListClient, bucket, prefix string) ([]string, error) {
	var keys []string
	err := ForEachObject(ctx, client, bucket, prefix, func(obj types.Object) error {
		keys = append(keys, aws.ToString(obj.Key))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}
