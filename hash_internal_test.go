package flow

import "testing"

type pair struct{ A, B string }

// %v flattens a struct's fields into one space-separated line, so these two
// distinct keys used to hash identically and land on the same worker. %#v keeps
// them apart.
func TestHashKeyDistinguishesFlattenedStructs(t *testing.T) {
	p := pair{"x y", "z"}
	q := pair{"x", "y z"}
	if hashKey(p) == hashKey(q) {
		t.Fatalf("%#v and %#v still hash alike (%d)", p, q, hashKey(p))
	}
}

// A uint64 key is the caller's own hash and must be used verbatim: that is the
// documented way to choose the distribution yourself.
func TestHashKeyPassesUint64Through(t *testing.T) {
	for _, want := range []uint64{0, 1, 42, 1 << 63, ^uint64(0)} {
		if got := hashKey(want); got != want {
			t.Errorf("hashKey(%d) = %d, want it unchanged", want, got)
		}
	}
}

// Distinct strings and ints must not fold together.
func TestHashKeySpreadsFastPaths(t *testing.T) {
	seen := map[uint64]string{}
	for _, s := range []string{"", "a", "b", "user-1", "user-2", "tenant/42"} {
		h := hashKey(s)
		if prev, dup := seen[h]; dup {
			t.Errorf("strings %q and %q collide at %d", prev, s, h)
		}
		seen[h] = s
	}
	ints := map[uint64]int{}
	for _, n := range []int{0, 1, 2, 1000, -1} {
		h := hashKey(n)
		if prev, dup := ints[h]; dup {
			t.Errorf("ints %d and %d collide at %d", prev, n, h)
		}
		ints[h] = n
	}
}

func BenchmarkHashKeyString(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = hashKey("user-1234")
	}
}

func BenchmarkHashKeyInt(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = hashKey(1234)
	}
}

func BenchmarkHashKeyUint64(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = hashKey(uint64(1234))
	}
}

// The fallback path, for keys with no fast path.
func BenchmarkHashKeyStruct(b *testing.B) {
	k := pair{"user-1234", "tenant"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = hashKey(k)
	}
}
