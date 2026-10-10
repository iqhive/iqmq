// Package stat implements the small set of statistics the benchmark gate
// needs: medians, geometric means, a two-sided Mann-Whitney U test and a
// nonparametric confidence interval for the median.
package stat

import (
	"math"
	"sort"
)

// Median returns the median of xs, or 0 for an empty slice.
func Median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	c := append([]float64(nil), xs...)
	sort.Float64s(c)
	n := len(c)
	if n%2 == 1 {
		return c[n/2]
	}
	return (c[n/2-1] + c[n/2]) / 2
}

// Mean returns the arithmetic mean of xs, or 0 for an empty slice.
func Mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := 0.0
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

// Geomean returns the geometric mean of the positive values in xs, or 0 when
// there are none.
func Geomean(xs []float64) float64 {
	s, n := 0.0, 0
	for _, x := range xs {
		if x > 0 {
			s += math.Log(x)
			n++
		}
	}
	if n == 0 {
		return 0
	}
	return math.Exp(s / float64(n))
}

// MedianCI returns a nonparametric ~95% confidence interval for the median
// of xs using order statistics. For fewer than 6 samples the interval is the
// full range.
func MedianCI(xs []float64) (lo, hi float64) {
	c := append([]float64(nil), xs...)
	sort.Float64s(c)
	n := len(c)
	if n == 0 {
		return 0, 0
	}
	if n < 6 {
		return c[0], c[n-1]
	}
	// Find the largest k with P(X <= k-1) <= 0.025 for X ~ Binomial(n, 0.5).
	k := 0
	cum := 0.0
	for i := range n {
		cum += binomPMF(n, i)
		if cum > 0.025 {
			break
		}
		k = i + 1
	}
	if k < 1 {
		k = 1
	}
	return c[k-1], c[n-k]
}

func binomPMF(n, i int) float64 {
	return math.Exp(lgamma(n+1)-lgamma(i+1)-lgamma(n-i+1)) * math.Pow(0.5, float64(n))
}

func lgamma(x int) float64 {
	v, _ := math.Lgamma(float64(x))
	return v
}

// MannWhitney performs a two-sided Mann-Whitney U test and returns the U
// statistic for a and the p-value. For samples of at most 20 without ties the
// exact distribution is used; otherwise the normal approximation with tie
// correction. A p-value of 1 is returned when either sample is empty.
func MannWhitney(a, b []float64) (u, p float64) {
	n1, n2 := len(a), len(b)
	if n1 == 0 || n2 == 0 {
		return 0, 1
	}
	type obs struct {
		v     float64
		group int
	}
	all := make([]obs, 0, n1+n2)
	for _, x := range a {
		all = append(all, obs{x, 0})
	}
	for _, x := range b {
		all = append(all, obs{x, 1})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].v < all[j].v })
	// Assign midranks and compute the tie correction term.
	ranks := make([]float64, len(all))
	ties := 0.0
	hasTies := false
	for i := 0; i < len(all); {
		j := i
		for j+1 < len(all) && all[j+1].v == all[i].v {
			j++
		}
		r := float64(i+j+2) / 2
		for k := i; k <= j; k++ {
			ranks[k] = r
		}
		if t := j - i + 1; t > 1 {
			hasTies = true
			ties += float64(t*t*t - t)
		}
		i = j + 1
	}
	r1 := 0.0
	for i, o := range all {
		if o.group == 0 {
			r1 += ranks[i]
		}
	}
	u1 := r1 - float64(n1*(n1+1))/2
	u2 := float64(n1*n2) - u1
	if !hasTies && n1 <= 20 && n2 <= 20 {
		umin := math.Min(u1, u2)
		p = 2 * exactCDF(n1, n2, int(math.Floor(umin)))
		if p > 1 {
			p = 1
		}
		return u1, p
	}
	n := float64(n1 + n2)
	mu := float64(n1*n2) / 2
	sigma := math.Sqrt(float64(n1*n2) / 12 * ((n + 1) - ties/(n*(n-1))))
	if sigma == 0 {
		return u1, 1
	}
	z := (math.Abs(u1-mu) - 0.5) / sigma
	if z < 0 {
		z = 0
	}
	p = 2 * (1 - normCDF(z))
	if p > 1 {
		p = 1
	}
	return u1, p
}

func normCDF(z float64) float64 { return 0.5 * math.Erfc(-z/math.Sqrt2) }

// exactCDF returns P(U <= u) for sample sizes m and n under the null
// hypothesis, computed by counting rank arrangements.
func exactCDF(m, n, u int) float64 {
	memo := map[[3]int]float64{}
	var count func(m, n, u int) float64
	count = func(m, n, u int) float64 {
		if u < 0 {
			return 0
		}
		if m == 0 || n == 0 {
			return 1
		}
		key := [3]int{m, n, u}
		if v, ok := memo[key]; ok {
			return v
		}
		v := count(m-1, n, u-n) + count(m, n-1, u)
		memo[key] = v
		return v
	}
	total := math.Exp(lgamma(m+n+1) - lgamma(m+1) - lgamma(n+1))
	return count(m, n, u) / total
}

// GeomeanRatioCI returns the 2.5th, 50th and 97.5th percentiles of the
// bootstrap distribution of
//
//	geomean_r(median(a[r])) / geomean_r(median(b[r]))
//
// over the paired rows r of two candidates, which is the 95% confidence
// interval step 2 asks `benchstat` for, lifted from one
// benchmark to the geomean the specification decides on. a[r] and b[r] are the samples
// of row r for the two candidates; rows with no sample on either side are
// skipped.
//
// The resampling is seeded, so two runs of `bakeoff decide` over one raw.txt
// produce the same interval: an ADR is rendered from the scorecard and must
// be reproducible from the same data.
func GeomeanRatioCI(a, b [][]float64, iters int, seed uint64) (lo, mid, hi float64) {
	if len(a) != len(b) || len(a) == 0 {
		return 0, 0, 0
	}
	if iters <= 0 {
		iters = 1000
	}
	rng := newPCG(seed)
	sample := make([]float64, iters)
	bufA := make([]float64, 0, 64)
	bufB := make([]float64, 0, 64)
	n := 0
	for i := range iters {
		sum, rows := 0.0, 0
		for r := range a {
			if len(a[r]) == 0 || len(b[r]) == 0 {
				continue
			}
			ma := resampleMedian(a[r], rng, &bufA)
			mb := resampleMedian(b[r], rng, &bufB)
			if ma <= 0 || mb <= 0 {
				continue
			}
			sum += math.Log(ma) - math.Log(mb)
			rows++
		}
		if rows == 0 {
			return 0, 0, 0
		}
		sample[i] = math.Exp(sum / float64(rows))
		n = i + 1
	}
	sample = sample[:n]
	sort.Float64s(sample)
	return quantile(sample, 0.025), quantile(sample, 0.5), quantile(sample, 0.975)
}

// resampleMedian draws len(xs) values from xs with replacement and returns
// their median, reusing buf.
func resampleMedian(xs []float64, rng *pcg, buf *[]float64) float64 {
	c := (*buf)[:0]
	for range xs {
		c = append(c, xs[rng.n(uint64(len(xs)))])
	}
	*buf = c
	sort.Float64s(c)
	m := len(c)
	if m%2 == 1 {
		return c[m/2]
	}
	return (c[m/2-1] + c[m/2]) / 2
}

// quantile is the linearly interpolated p-quantile of a sorted slice.
func quantile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}
	pos := p * float64(n-1)
	i := int(pos)
	if i >= n-1 {
		return sorted[n-1]
	}
	frac := pos - float64(i)
	return sorted[i] + frac*(sorted[i+1]-sorted[i])
}

// pcg is the 64-bit PCG-XSH-RR generator, written out rather than taken from
// math/rand/v2 so that the sequence is fixed by this file and a bootstrap
// interval recorded in an ADR stays reproducible across Go releases.
type pcg struct{ state, inc uint64 }

func newPCG(seed uint64) *pcg {
	p := &pcg{state: 0, inc: (seed << 1) | 1}
	p.next()
	p.state += seed
	p.next()
	return p
}

func (p *pcg) next() uint32 {
	old := p.state
	p.state = old*6364136223846793005 + p.inc
	xorshifted := uint32(((old >> 18) ^ old) >> 27)
	rot := uint32(old >> 59)
	return (xorshifted >> rot) | (xorshifted << ((32 - rot) & 31))
}

// n returns a value in [0, bound) with the low-bias rejection method.
func (p *pcg) n(bound uint64) int {
	if bound <= 1 {
		return 0
	}
	b := uint32(bound)
	threshold := uint32(-int32(b)) % b
	for {
		r := p.next()
		if r >= threshold {
			return int(r % b)
		}
	}
}
