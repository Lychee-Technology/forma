// Package txseal seals forma.Tx (#648).
//
// forma.Tx embeds Sealed, whose one method is unexported, so a type satisfies
// forma.Tx only by embedding Seal, and only packages inside the forma module
// can import this package. That keeps Tx open to new methods: a later child of
// #593 adds a method to Tx without breaking any caller, because no caller can
// have implemented it.
//
// The seal lives here rather than as an unexported method of package forma
// because Forma's implementation of Tx lives in package internal. A method
// declared in package forma could be acquired from outside it only by
// embedding forma.Tx itself, and an embedded interface also "implements" every
// method added to Tx later, which would turn a forgotten method into a nil
// dereference at run time instead of a compile error.
package txseal

// Sealed is embedded in forma.Tx.
type Sealed interface {
	sealedTx()
}

// Seal is the zero-size marker Forma's Tx implementation embeds.
type Seal struct{}

func (Seal) sealedTx() {}
