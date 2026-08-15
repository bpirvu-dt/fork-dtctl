package exec

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/dynatrace-oss/dtctl/pkg/client"
	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/httpclient"
)

var errReplayProbeIncomplete = errors.New("replay probe did not complete synchronously")

func newReplayProbeHandler(c *client.Client, clientContext string) *sdkquery.Handler {
	if c == nil {
		return nil
	}
	return sdkquery.NewHandler(httpclient.Wrap(c.HTTP())).
		WithHeaders(map[string]string{"dt-client-context": dtClientContextHeader(clientContext)}).
		WithFirstRateLimitResponse()
}

func executeReplayProbe(ctx context.Context, handler *sdkquery.Handler, timeout time.Duration, request sdkquery.ExecuteRequest) (*sdkquery.Response, error) {
	if handler == nil {
		return nil, errors.New("replay probe handler is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	response, err := handler.Execute(probeCtx, request)
	if err != nil {
		return nil, err
	}
	if response == nil || !strings.EqualFold(response.State, "SUCCEEDED") {
		return response, errReplayProbeIncomplete
	}
	return response, nil
}

func replayProbeRecords(response *sdkquery.Response) []map[string]interface{} {
	if response == nil {
		return nil
	}
	records := response.Records
	if response.Result != nil {
		records = response.Result.Records
	}
	return records
}
