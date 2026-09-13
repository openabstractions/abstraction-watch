package watch

import (
	"context"
	api "github.com/openabstractions/abstraction-watch/go/abstraction/watch/api"
	"testing"
	"time"
)

func TestDescriptorMetadataPreservesNativePayload(t *testing.T) {
	payload := &struct{ Name string }{"native object"}
	sub := Push(payload, "first", time.Hour)
	defer sub.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	notice, err := sub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if notice.Now != payload {
		t.Fatal("generic payload identity changed")
	}
	metadata := api.NoticeMetadata{Quiet: notice.Quiet, SilenceNs: int64(notice.Silence)}
	decoded, err := api.Decode(api.Encode(&metadata))
	if err != nil || *decoded != metadata {
		t.Fatalf("metadata: %+v %v", decoded, err)
	}
	if sub.Current() != payload {
		t.Fatal("metadata serialization changed native subscription")
	}
}
