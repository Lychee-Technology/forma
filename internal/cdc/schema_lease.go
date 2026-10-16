package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// The Aurora DSQL engine's schema lock (#633). DSQL has no advisory locks,
// and a connection there is not a session a lock could live in, so a holder
// writes the schema's row in the schema-lease table instead: its holder
// token and an expiry on the database clock. It renews the expiry while it
// works, and deletes the row on release.
//
// Expiry replaces "the lock ends with the session": a holder that dies
// leaves a row that expires, and the next holder takes it over. A holder
// that is alive but cannot renew must stop before that takeover, so its held
// context ends at the last renewal it sent plus the TTL minus one renewal
// interval. The database set that renewal's expiry no earlier than when it
// was sent, so the context ends at least one renewal interval before another
// holder can take the lease. A statement already sent is not recalled, the
// same limit a PostgreSQL session lock has when its connection drops.
const (
	defaultLeaseTTL        = time.Minute
	defaultLeaseRenewEvery = 15 * time.Second
	leaseReleaseTimeout    = 5 * time.Second
)

// leaseSchemaLocker takes schema leases in one schema-lease table.
type leaseSchemaLocker struct {
	db    *sql.DB
	table string // sanitized
	// ttl is how long past the database clock an acquire or a renewal sets
	// the expiry; renewEvery is the renewal interval, at most a third of
	// ttl so a holder can miss a renewal and still keep its lease.
	ttl        time.Duration
	renewEvery time.Duration
}

func newLeaseSchemaLocker(db *sql.DB, table string, ttl, renewEvery time.Duration) *leaseSchemaLocker {
	return &leaseSchemaLocker{db: db, table: sanitizeIdentifier(table), ttl: ttl, renewEvery: renewEvery}
}

// validFor is how long after sending an acquire or a renewal the holder may
// still work if no later renewal succeeds.
func (l *leaseSchemaLocker) validFor() time.Duration { return l.ttl - l.renewEvery }

// TryLock takes schemaID's lease when no live lease holds it. A concurrent
// acquirer that wins the race leaves this one ok false, never an error.
func (l *leaseSchemaLocker) TryLock(ctx context.Context, schemaID int16) (context.Context, func(), bool, error) {
	holder := uuid.New()
	sent := time.Now()
	ok, err := l.acquire(ctx, schemaID, holder)
	if err != nil || !ok {
		return nil, nil, false, err
	}
	held, cancel := context.WithCancelCause(ctx)
	h := &leaseHold{locker: l, schemaID: schemaID, holder: holder, cancel: cancel, done: make(chan struct{})}
	go h.renewUntilReleased(held, sent.Add(l.validFor()))
	return held, h.release, true, nil
}

// acquire inserts schemaID's lease, or takes over one that has expired. The
// upsert returns no row while a live lease holds the schema.
func (l *leaseSchemaLocker) acquire(ctx context.Context, schemaID int16, holder uuid.UUID) (bool, error) {
	query := fmt.Sprintf(`INSERT INTO %[1]s AS l (schema_id, holder, expires_at) VALUES ($1, $2, %[2]s + $3)
		ON CONFLICT (schema_id) DO UPDATE SET holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at
		WHERE l.expires_at <= %[2]s
		RETURNING l.holder`, l.table, sqlgen.ServerTimeMillisSQL)
	var got uuid.UUID
	err := l.db.QueryRowContext(ctx, query, schemaID, holder, l.ttl.Milliseconds()).Scan(&got)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case isLeaseRace(err):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("acquire schema %d lease in %s: %w", schemaID, l.table, err)
	}
	return got == holder, nil
}

// isLeaseRace reports the outcomes of losing a race to write one lease row:
// 40001 on DSQL and under REPEATABLE READ, and 23505 should DSQL answer two
// inserts of one new key that way.
func isLeaseRace(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "23505")
}

// renew extends holder's lease of schemaID, and answers false when holder no
// longer holds a live one.
func (l *leaseSchemaLocker) renew(ctx context.Context, schemaID int16, holder uuid.UUID) (bool, error) {
	query := fmt.Sprintf(`UPDATE %[1]s SET expires_at = %[2]s + $3
		WHERE schema_id = $1 AND holder = $2 AND expires_at > %[2]s`, l.table, sqlgen.ServerTimeMillisSQL)
	res, err := l.db.ExecContext(ctx, query, schemaID, holder, l.ttl.Milliseconds())
	if err != nil {
		return false, fmt.Errorf("renew schema %d lease in %s: %w", schemaID, l.table, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("count renewed schema %d leases: %w", schemaID, err)
	}
	return n == 1, nil
}

// leaseHold is one held lease and its renewal loop.
type leaseHold struct {
	locker   *leaseSchemaLocker
	schemaID int16
	holder   uuid.UUID
	cancel   context.CancelCauseFunc
	done     chan struct{}
	once     sync.Once
}

// renewUntilReleased renews the lease every renewEvery until held ends, and
// ends held with ErrSchemaLockLost when the lease is gone or validUntil
// passes without a successful renewal. The deadline runs on a timer of its
// own, so a renewal stuck on the database cannot hold held past it. A failed
// renewal is tried again at the next tick: a transient error costs nothing
// while the lease is valid.
func (h *leaseHold) renewUntilReleased(held context.Context, validUntil time.Time) {
	defer close(h.done)
	expired := time.AfterFunc(time.Until(validUntil), func() {
		h.cancel(fmt.Errorf("%w: schema %d's lease was not renewed within %s", ErrSchemaLockLost, h.schemaID, h.locker.validFor()))
	})
	defer expired.Stop()
	ticker := time.NewTicker(h.locker.renewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-held.Done():
			return
		case <-ticker.C:
		}
		sent := time.Now()
		ok, err := h.renewOnce(held)
		if err != nil {
			continue
		}
		if !ok {
			h.cancel(fmt.Errorf("%w: schema %d's lease is held by another holder or gone", ErrSchemaLockLost, h.schemaID))
			return
		}
		expired.Reset(time.Until(sent.Add(h.locker.validFor())))
	}
}

// renewOnce sends one renewal, cut short when held ends.
func (h *leaseHold) renewOnce(held context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(held, h.locker.renewEvery)
	defer cancel()
	return h.locker.renew(ctx, h.schemaID, h.holder)
}

// release ends held, waits for the renewal loop, and deletes the lease if
// this holder still holds it. A failed delete leaves a lease that expires
// after the TTL.
func (h *leaseHold) release() {
	h.once.Do(func() {
		h.cancel(nil)
		<-h.done
		ctx, cancel := context.WithTimeout(context.Background(), leaseReleaseTimeout)
		defer cancel()
		query := fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1 AND holder = $2", h.locker.table)
		_, _ = h.locker.db.ExecContext(ctx, query, h.schemaID, h.holder)
	})
}
