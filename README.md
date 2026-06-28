# clawpro-go

Official Go client for the **ClawPro** Instagram outbound API — connect sending accounts, run competitor-audience campaigns, score leads, manage webhooks, and read usage.

```bash
go get github.com/Bojale-Labs/clawpro-go@latest
```

Requires Go 1.21+. Get an API key from the [developer portal](https://tryclawpro.com) → **API & Developers → API Keys**.

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"os"

	clawpro "github.com/Bojale-Labs/clawpro-go"
)

func main() {
	c := clawpro.New(os.Getenv("CLAWPRO_API_KEY"))
	ctx := context.Background()

	account, err := c.CreateAccount(ctx, clawpro.CreateAccountParams{Username: "burner_account", Country: "gb"})
	if err != nil {
		panic(err)
	}

	campaign, _ := c.CreateCampaign(ctx, clawpro.CreateCampaignParams{
		AccountID:     account.ID,
		Name:          "Founders engaging with X",
		Targets:       []string{"@influencer1"},
		Offer:         "We help B2B founders book demos via Instagram outbound.",
		DailyDMTarget: 20,
	})
	c.RunCampaign(ctx, campaign.ID)

	usage, _ := c.Usage(ctx)
	fmt.Printf("%+v\n", usage)
}
```

## Configuration

```go
c := clawpro.New(apiKey,
	clawpro.WithBaseURL("https://api.tryclawpro.com"),
	clawpro.WithMaxRetries(3),
	clawpro.WithHTTPClient(myHTTPClient),
)
```

Requests authenticate with the `X-API-Key` header. Transient failures (network,
`429` honoring `Retry-After`, `5xx`) are retried with exponential backoff for
idempotent calls.

## Errors

Non-2xx responses return a `*clawpro.Error`:

```go
if _, err := c.CreateAccount(ctx, params); err != nil {
	var apiErr *clawpro.Error
	if errors.As(err, &apiErr) {
		fmt.Println(apiErr.Status, apiErr.Message, apiErr.RequestID)
	}
}
```

## License

MIT
