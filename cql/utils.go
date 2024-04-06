package cql

func deref[T any](ref *T, fallback T) T {
	if ref == nil {
		return fallback
	}
	return *ref
}
