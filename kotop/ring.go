package kotop

type Ring struct {
	idx  int
	buf  []int
	size int
}

func newRing(size int) *Ring {
	return &Ring{
		buf:  make([]int, size),
		size: size,
	}
}

func (r *Ring) add(a int) {
	r.buf[r.idx] = a
	r.idx++
	if r.idx >= r.size {
		r.idx = 0
	}
}

// dump most recent n added
func (r *Ring) dump(n int) []int {
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
