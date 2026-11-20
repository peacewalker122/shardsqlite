package readgate

type Gate struct {
	applied func() uint64
}

func New(applied func() uint64) *Gate {
	return &Gate{applied: applied}
}

func (g *Gate) Wait() error {
	return nil
}

func (g *Gate) AppliedIndex() uint64 {
	return g.applied()
}
