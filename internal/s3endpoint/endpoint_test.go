package s3endpoint

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestClientEndpointRejectsAmbiguousPaths verifies R15-F1 directly on
// ClientEndpoint: custom endpoints with repeated or ambiguous path slashes
// never normalize to the same binding as a clean path.
func TestClientEndpointRejectsAmbiguousPaths(t *testing.T) {
	ctx := context.Background()
	cleanClient := func(base string, pathStyle bool) *s3.Client {
		return s3.New(s3.Options{
			Region:       "us-east-1",
			BaseEndpoint: aws.String(base),
			UsePathStyle: pathStyle,
		})
	}
	epTenant, err := ClientEndpoint(ctx, cleanClient("https://s3.storage.example/tenant", false), "lake")
	require.NoError(t, err)
	require.Equal(t, "https://lake.s3.storage.example/tenant", epTenant)

	epPath, err := ClientEndpoint(ctx, cleanClient("https://s3.storage.example/tenant", true), "lake")
	require.NoError(t, err)
	require.Equal(t, "https://s3.storage.example/tenant/lake", epPath)
	require.NotEqual(t, epTenant, epPath)

	for _, bad := range []string{
		"https://s3.storage.example/tenant//",
		"https://s3.storage.example//",
		"https://s3.storage.example/tenant/..",
		"https://s3.storage.example/tenant%2Fa",
	} {
		for _, pathStyle := range []bool{false, true} {
			_, err := ClientEndpoint(ctx, cleanClient(bad, pathStyle), "lake")
			require.ErrorIs(t, err, ErrUnknownDestination, "base=%q pathStyle=%v", bad, pathStyle)
		}
	}
}
