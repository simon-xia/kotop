package kotop

type ring struct {
	idx  int
	buf  []int
	size int
}

func newRing(size int) *ring {
	return &ring{
		buf:  make([]int, size),
		size: size,
	}
}

func (r *ring) add(a int) {
	r.buf[r.idx] = a
	r.idx++
	if r.idx >= r.size {
		r.idx = 0
	}
}

// dump most recent n added
func (r *ring) dump(n int) []int {
	if n > r.size {
		n = r.size
	}
	dump := make([]int, n)
	j := r.idx

	for i := 0; i < n; i++ {
		dump[n-1-i] = r.buf[j]
		j--
		if j < 0 {
			j = r.size - 1
		}
	}

	return dump
}
