package manifest

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type testAPIErr struct{ code string }

func (e *testAPIErr) Error() string                 { return "api error " + e.code }
func (e *testAPIErr) ErrorCode() string             { return e.code }
func (e *testAPIErr) ErrorMessage() string          { return e.code }
func (e *testAPIErr) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

func TestIsPreconditionFailed(t *testing.T) {
	respErr := &awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusPreconditionFailed}},
		Err:      errors.New("412"),
	}}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"api error PreconditionFailed", &testAPIErr{code: "PreconditionFailed"}, true},
		{"wrapped api error", fmt.Errorf("put manifest: %w", &testAPIErr{code: "PreconditionFailed"}), true},
		{"other api error", &testAPIErr{code: "NoSuchKey"}, false},
		{"http 412 response error", respErr, true},
		{"plain error", errors.New("connection reset"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPreconditionFailed(tt.err); got != tt.want {
				t.Fatalf("IsPreconditionFailed(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
