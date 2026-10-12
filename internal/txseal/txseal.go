// Package txseal seals forma.Tx (#648).
//
// forma.Tx embeds Sealed, whose one method is unexported, so a type satisfies
// forma.Tx only if it gets that method by embedding: Seal, which only packages
// inside the forma module can import, or a forma.Tx. A caller therefore cannot
// implement Tx from scratch, and a later child of #593 can add a method to Tx
// without breaking any caller's build: a caller's type that embeds a forma.Tx
// gets the new method from the embedded value.
//
// The seal does not make Forma the only implementer. A caller's wrapper or
// test double that embeds a forma.Tx satisfies it, and the embedded value may
// be nil. Forma must therefore never take a forma.Tx back from a caller and
// treat it as its own: a Tx goes out of InTx to fn, never into Forma.
//
// The seal lives here rather than as an unexported method of package forma
// because Forma's implementation of Tx lives in package internal. With a
// method of package forma, that implementation could satisfy forma.Tx only by
// embedding forma.Tx itself, and the embedded interface would also "implement"
// every method a later child adds, turning one the child forgot to write into
// a nil dereference at run time instead of a compile error.
package txseal

// Sealed is embedded in forma.Tx.
type Sealed interface {
	sealedTx()
}

// Seal is the zero-size marker Forma's Tx implementation embeds.
type Seal struct{}

func (Seal) sealedTx() {}
