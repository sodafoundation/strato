package driver

type Closer interface {
	Close()
}

var closers []Closer

func AddCloser(closer Closer) {
	closers = append(closers, closer)
}

func FreeCloser() {
	for _, c := range closers {
		c.Close()
	}
}
