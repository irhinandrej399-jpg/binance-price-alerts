package live

// Status is an in-memory observation of the current generation. AppliedC is a
// successfully read durable checkpoint, not a broker offset or API pin progress.
// Head is the last validated API head. Lag is nil until head is known; it counts
// journal positions, not subscriptions, prices, elapsed time, or pending sends.
type Status struct {
	Ready    bool    `json:"ready"`
	AppliedC uint64  `json:"applied_c"`
	Head     uint64  `json:"head"`
	Lag      *uint64 `json:"lag"`
}

func (c *Coordinator) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := Status{Ready: c.readyLocked(), AppliedC: c.appliedC, Head: c.head}
	if c.headKnown {
		lag := uint64(0)
		if c.head > c.appliedC {
			lag = c.head - c.appliedC
		}
		s.Lag = &lag
	}
	return s
}

func (c *Coordinator) observeCheckpoint(pos uint64) {
	c.mu.Lock()
	c.appliedC = pos
	c.mu.Unlock()
}
