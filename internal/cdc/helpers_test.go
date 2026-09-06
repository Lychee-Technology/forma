package cdc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	testTmpKey   = "cdc/7/_tmp/file.parquet"
	testFinalKey = "cdc/7/delta-file.parquet"
)

// #226: a failed CopyObject strands the tmp object forever (the retry uses
// fresh UUIDv7 keys), so CopyTmpToFinal must best-effort delete its own tmp
// before surfacing the copy error.
func TestCopyTmpToFinal_DeletesTmpWhenCopyFails(t *testing.T) {
	client := &recordingS3Client{copyErr: errors.New("copy failed")}

	err := CopyTmpToFinal(context.Background(), client, "test-bucket", testTmpKey, testFinalKey, zap.NewNop())
	require.Error(t, err)
	require.Contains(t, err.Error(), "copy tmp->final")
	require.Equal(t, []string{testTmpKey}, client.deletedKeys)
}

// The cleanup is best-effort: a delete failure on top of the copy failure
// must not mask the copy error (the caller retries the whole batch; the
// residual orphan is reclaimed by manifest-reconcile --gc).
func TestCopyTmpToFinal_CopyErrorSurvivesFailedCleanup(t *testing.T) {
	copyErr := errors.New("copy failed")
	client := &recordingS3Client{copyErr: copyErr, deleteErr: errors.New("delete failed")}

	err := CopyTmpToFinal(context.Background(), client, "test-bucket", testTmpKey, testFinalKey, zap.NewNop())
	require.ErrorIs(t, err, copyErr)
	require.Equal(t, []string{testTmpKey}, client.deletedKeys)
}

// Success path unchanged: copy then exactly one delete of the tmp key; a
// swallowed delete failure still returns nil (flush success is correct, the
// residue is GC's problem).
func TestCopyTmpToFinal_SuccessDeletesTmpOnce(t *testing.T) {
	client := &recordingS3Client{}

	err := CopyTmpToFinal(context.Background(), client, "test-bucket", testTmpKey, testFinalKey, zap.NewNop())
	require.NoError(t, err)
	require.Equal(t, []string{testTmpKey}, client.deletedKeys)
}

func TestCopyTmpToFinal_SwallowsPostCopyDeleteFailure(t *testing.T) {
	client := &recordingS3Client{deleteErr: errors.New("delete failed")}

	err := CopyTmpToFinal(context.Background(), client, "test-bucket", testTmpKey, testFinalKey, zap.NewNop())
	require.NoError(t, err)
	require.Equal(t, []string{testTmpKey}, client.deletedKeys)
}

// #516: an empty data prefix makes cdc.BuildTempPath emit "/1/_tmp/<uuid>"
// and the exporter writes exactly that key. CopySource must name it verbatim
// ("<bucket>//1/_tmp/..."); trimming the slash would copy from the sibling
// "1/_tmp/..." that was never written and fail every empty-prefix promotion.
func TestCopyTmpToFinal_LeadingSlashTmpKeyCopiesVerbatim(t *testing.T) {
	const (
		tmpKey   = "/1/_tmp/file.parquet"
		finalKey = "/1/delta-file.parquet"
	)
	client := &recordingS3Client{}

	err := CopyTmpToFinal(context.Background(), client, "test-bucket", tmpKey, finalKey, zap.NewNop())
	require.NoError(t, err)
	require.Equal(t, []string{"test-bucket//1/_tmp/file.parquet"}, client.copySources)
	require.Equal(t, []string{finalKey}, client.copiedKeys)
	require.Equal(t, []string{tmpKey}, client.deletedKeys, "the tmp cleanup names the verbatim key too")
}

// The ordinary shape is unchanged: a prefixed tmp key renders "<bucket>/<key>".
func TestCopyTmpToFinal_PrefixedTmpKeyCopySource(t *testing.T) {
	client := &recordingS3Client{}

	err := CopyTmpToFinal(context.Background(), client, "test-bucket", testTmpKey, testFinalKey, zap.NewNop())
	require.NoError(t, err)
	require.Equal(t, []string{"test-bucket/" + testTmpKey}, client.copySources)
	require.Equal(t, []string{testFinalKey}, client.copiedKeys)
}
