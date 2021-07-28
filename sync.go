package async

type WG struct {
	I int
}

func (wg *WG) Add(i int) {
	wg.I += i
}

func (wg *WG) Done() {
	wg.I--
}

func (wg *WG) Wait(label string) Stmt {
	return WaitFor(label, wg.I == 0, nil)
}
