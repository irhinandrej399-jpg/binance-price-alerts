package price

// Satisfied checks the current price against a fixed threshold: Up includes
// prices >= threshold, Down includes prices <= threshold. Unknown directions
// return false. The caller supplies a valid market quote and threshold.
//
// No previous price or result is retained. A true result describes only this
// price; delivery must check the latest price again before each attempt.
func Satisfied(current, threshold Price, direction Direction) bool {
	switch direction {
	case Up:
		return current.Cmp(threshold) >= 0
	case Down:
		return current.Cmp(threshold) <= 0
	default:
		return false
	}
}
