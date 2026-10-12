package factory

import (
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
)

// NewTransactor returns a Transactor sharing em's pool, schema registry,
// validator, transformer and configuration (#648). em must be a manager this
// package built; any other EntityManager, including one that wraps a factory
// manager, answers ErrInvalidInput. The Transactor holds no resources of its
// own: closing em's pool ends it, and em.Close does not.
//
//	txr, err := factory.NewTransactor(em)
//	err = txr.InTx(ctx, func(ctx context.Context, tx forma.Tx) error {
//		if _, err := tx.Update(ctx, instanceUpdate); err != nil {
//			return fmt.Errorf("advance instance: %w", err)
//		}
//		_, err := tx.Create(ctx, outboxMessage)
//		return err
//	})
//	if forma.IsRetryable(err) {
//		// run the whole function again
//	}
//
// See docs/transactions.md.
func NewTransactor(em forma.EntityManager) (forma.Transactor, error) {
	return internal.NewTransactor(em)
}
