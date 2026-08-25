module riffdb.dev/app-baseline-golang

go 1.24

require (
	github.com/jackc/pgx/v5 v5.7.6
	riffdb.dev/application v0.0.0
	riffdb.dev/ticketdesk v0.0.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/text v0.24.0 // indirect
)

replace riffdb.dev/application => ../../../clients/go/runtime

replace riffdb.dev/ticketdesk => ../../../clients/go/ticketdesk
