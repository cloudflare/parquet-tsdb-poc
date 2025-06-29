// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"unsafe"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

type Constraint interface {
	fmt.Stringer

	// filter returns a set of non-overlapping increasing row indexes that may satisfy the constraint.
	filter(ctx context.Context, rg parquet.RowGroup, primary bool, rr []rowRange) ([]rowRange, error)
	// init initializes the constraint with respect to the file schema and projections.
	init(s *parquet.Schema) error
	// path is the path for the column that is constrained
	path() string
}

func matchersToConstraint(matchers ...*labels.Matcher) ([]Constraint, error) {
	r := make([]Constraint, 0, len(matchers))
	for _, matcher := range matchers {
		switch matcher.Type {
		case labels.MatchEqual:
			r = append(r, Equal(schema.LabelNameToColumn(matcher.Name), parquet.ValueOf(matcher.Value)))
		case labels.MatchNotEqual:
			r = append(r, Not(Equal(schema.LabelNameToColumn(matcher.Name), parquet.ValueOf(matcher.Value))))
		case labels.MatchRegexp:
			res, err := labels.NewFastRegexMatcher(matcher.Value)
			if err != nil {
				return nil, err
			}
			set := res.SetMatches()
			if len(set) == 1 {
				r = append(r, Equal(schema.LabelNameToColumn(matcher.Name), parquet.ValueOf(set[0])))
			} else {
				r = append(r, Regex(schema.LabelNameToColumn(matcher.Name), res))
			}
		case labels.MatchNotRegexp:
			res, err := labels.NewFastRegexMatcher(matcher.Value)
			if err != nil {
				return nil, err
			}
			set := res.SetMatches()
			if len(set) == 1 {
				r = append(r, Not(Equal(schema.LabelNameToColumn(matcher.Name), parquet.ValueOf(set[0]))))
			} else {
				r = append(r, Not(Regex(schema.LabelNameToColumn(matcher.Name), res)))
			}
		default:
			return nil, fmt.Errorf("unsupported matcher type %s", matcher.Type)
		}
	}
	return r, nil
}

func initialize(s *parquet.Schema, cs ...Constraint) error {
	for i := range cs {
		if err := cs[i].init(s); err != nil {
			return fmt.Errorf("unable to initialize constraint %d: %w", i, err)
		}
	}
	return nil
}

func filter(ctx context.Context, rg parquet.RowGroup, cs ...Constraint) ([]rowRange, error) {
	// Constraints for sorting columns are cheaper to evaluate, so we sort them first.
	sc := rg.SortingColumns()

	var n int
	for i := range sc {
		if n == len(cs) {
			break
		}
		for j := range cs {
			if cs[j].path() == sc[i].Path()[0] {
				cs[n], cs[j] = cs[j], cs[n]
				n++
			}
		}
	}
	var err error
	rr := []rowRange{{from: int64(0), count: rg.NumRows()}}
	for i := range cs {
		isPrimary := len(sc) > 0 && cs[i].path() == sc[0].Path()[0]
		rr, err = cs[i].filter(ctx, rg, isPrimary, rr)
		if err != nil {
			return nil, fmt.Errorf("unable to filter with constraint %d: %w", i, err)
		}
	}
	return rr, nil
}

type equalConstraint struct {
	pth string

	val parquet.Value

	comp func(l, r parquet.Value) int
}

func (ec *equalConstraint) String() string {
	return fmt.Sprintf("equal(%q,%q)", ec.pth, ec.val)
}

func Equal(path string, value parquet.Value) Constraint {
	return &equalConstraint{pth: path, val: value}
}

func (ec *equalConstraint) filter(ctx context.Context, rg parquet.RowGroup, primary bool, rr []rowRange) ([]rowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	method := methodFromContext(ctx)

	from, to := rr[0].from, rr[len(rr)-1].from+rr[len(rr)-1].count

	col, ok := rg.Schema().Lookup(ec.path())
	if !ok {
		// If match empty, return rr (filter nothing)
		// otherwise return empty
		if ec.matches(parquet.ValueOf("")) {
			return rr, nil
		}
		return []rowRange{}, nil
	}
	cc := rg.ColumnChunks()[col.ColumnIndex]

	if skip, err := ec.skipByBloomfilter(cc); err != nil {
		return nil, fmt.Errorf("unable to skip by bloomfilter: %w", err)
	} else if skip {
		return nil, nil
	}

	pgs := cc.Pages()
	defer func() { _ = pgs.Close() }()

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	var (
		symbols = new(symbolTable)
		res     = make([]rowRange, 0)
	)
	for i := range cidx.NumPages() {
		// If page does not intersect from, to; we can immediately discard it
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		// Page intersects [from, to] but we might be able to discard it with statistics
		if cidx.NullPage(i) {
			if ec.matches(parquet.ValueOf("")) {
				res = append(res, rowRange{pfrom, pcount})
			}
			continue
		}

		// If we are not matching the empty string ( which would be satisfied by Null too ), we can
		// use page statistics to skip rows
		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		if !ec.matches(parquet.ValueOf("")) && !maxv.IsNull() && ec.comp(ec.val, maxv) > 0 {
			if cidx.IsDescending() {
				break
			}
			continue
		}
		if !ec.matches(parquet.ValueOf("")) && !minv.IsNull() && ec.comp(ec.val, minv) < 0 {
			if cidx.IsAscending() {
				break
			}
			continue
		}
		// We cannot discard the page through statistics but we might need to read it to see if it has the value
		if err := pgs.SeekToRow(pfrom); err != nil {
			return nil, fmt.Errorf("unable to seek to row: %w", err)
		}
		pg, err := pgs.ReadPage()
		if err != nil {
			return nil, fmt.Errorf("unable to read page: %w", err)
		}

		symbols.Reset(pg)

		pagesScanned.WithLabelValues(ec.path(), scanEqual, method).Add(1)

		// The page has the value, we need to find the matching row ranges
		n := int(pg.NumRows())
		bl := int(max(pfrom, from) - pfrom)
		br := n - int(pto-min(pto, to))
		var l, r int
		switch {
		case cidx.IsAscending() && primary:
			l = sort.Search(n, func(i int) bool { return ec.comp(ec.val, symbols.Get(i)) <= 0 })
			r = sort.Search(n, func(i int) bool { return ec.comp(ec.val, symbols.Get(i)) < 0 })

			if lv, rv := max(bl, l), min(br, r); rv > lv {
				res = append(res, rowRange{pfrom + int64(lv), int64(rv - lv)})
			}
		default:
			off, count := bl, 0
			for j := bl; j < br; j++ {
				if !ec.matches(symbols.Get(j)) {
					if count != 0 {
						res = append(res, rowRange{pfrom + int64(off), int64(count)})
					}
					off, count = j, 0
				} else {
					if count == 0 {
						off = j
					}
					count++
				}
			}
			if count != 0 {
				res = append(res, rowRange{pfrom + int64(off), int64(count)})
			}
		}
	}
	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (ec *equalConstraint) init(s *parquet.Schema) error {
	c, ok := s.Lookup(ec.path())
	if !ok {
		return nil
	}
	stringKind := parquet.String().Type().Kind()
	if ec.val.Kind() != stringKind {
		return fmt.Errorf("schema: can only search string kind, got: %s", ec.val.Kind())
	}
	if c.Node.Type().Kind() != stringKind {
		return fmt.Errorf("schema: cannot search value of kind %s in column of kind %s", stringKind, c.Node.Type().Kind())
	}
	ec.comp = c.Node.Type().Compare
	return nil
}

func (ec *equalConstraint) path() string {
	return ec.pth
}

func (ec *equalConstraint) matches(v parquet.Value) bool {
	return bytes.Equal(v.ByteArray(), ec.val.ByteArray())
}

func (ec *equalConstraint) skipByBloomfilter(cc parquet.ColumnChunk) (bool, error) {
	bf := cc.BloomFilter()
	if bf == nil {
		return false, nil
	}
	ok, err := bf.Check(ec.val)
	if err != nil {
		return false, fmt.Errorf("unable to check bloomfilter: %w", err)
	}
	return !ok, nil
}

func Regex(path string, r *labels.FastRegexMatcher) Constraint {
	return &regexConstraint{pth: path, cache: make(map[parquet.Value]bool), r: r}
}

type regexConstraint struct {
	pth   string
	cache map[parquet.Value]bool

	r *labels.FastRegexMatcher
}

func (rc *regexConstraint) String() string {
	return fmt.Sprintf("regex(%v,%v)", rc.pth, rc.r.GetRegexString())
}

func (rc *regexConstraint) filter(ctx context.Context, rg parquet.RowGroup, _ bool, rr []rowRange) ([]rowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	method := methodFromContext(ctx)

	from, to := rr[0].from, rr[len(rr)-1].from+rr[len(rr)-1].count

	col, ok := rg.Schema().Lookup(rc.path())
	if !ok {
		// If match empty, return rr (filter nothing)
		// otherwise return empty
		if rc.matches(parquet.ValueOf("")) {
			return rr, nil
		}
		return []rowRange{}, nil
	}
	cc := rg.ColumnChunks()[col.ColumnIndex]

	pgs := cc.Pages()
	defer func() { _ = pgs.Close() }()

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	var (
		symbols = new(symbolTable)
		res     = make([]rowRange, 0)
	)
	for i := range cidx.NumPages() {
		// If page does not intersect from, to; we can immediately discard it
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		// Page intersects [from, to] but we might be able to discard it with statistics
		if cidx.NullPage(i) {
			if rc.matches(parquet.ValueOf("")) {
				res = append(res, rowRange{pfrom, pcount})
			}
			continue
		}
		// TODO: use setmatches / prefix for statistics

		// We cannot discard the page through statistics but we might need to read it to see if it has the value
		if err := pgs.SeekToRow(pfrom); err != nil {
			return nil, fmt.Errorf("unable to seek to row: %w", err)
		}
		pg, err := pgs.ReadPage()
		if err != nil {
			return nil, fmt.Errorf("unable to read page: %w", err)
		}

		symbols.Reset(pg)

		pagesScanned.WithLabelValues(rc.path(), scanRegex, method).Add(1)

		// The page has the value, we need to find the matching row ranges
		n := int(pg.NumRows())
		bl := int(max(pfrom, from) - pfrom)
		br := n - int(pto-min(pto, to))
		off, count := bl, 0
		for j := bl; j < br; j++ {
			if !rc.matches(symbols.Get(j)) {
				if count != 0 {
					res = append(res, rowRange{pfrom + int64(off), int64(count)})
				}
				off, count = j, 0
			} else {
				if count == 0 {
					off = j
				}
				count++
			}
		}
		if count != 0 {
			res = append(res, rowRange{pfrom + int64(off), int64(count)})
		}
	}
	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (rc *regexConstraint) init(s *parquet.Schema) error {
	c, ok := s.Lookup(rc.path())
	if !ok {
		return nil
	}
	if stringKind := parquet.String().Type().Kind(); c.Node.Type().Kind() != stringKind {
		return fmt.Errorf("schema: cannot search value of kind %s in column of kind %s", stringKind, c.Node.Type().Kind())
	}
	rc.cache = make(map[parquet.Value]bool)
	return nil
}

func (rc *regexConstraint) path() string {
	return rc.pth
}

func (rc *regexConstraint) matches(v parquet.Value) bool {
	accept, seen := rc.cache[v]
	if !seen {
		accept = rc.r.MatchString(yoloString(v.ByteArray()))
		rc.cache[v] = accept
	}
	return accept
}

func yoloString(buf []byte) string {
	return *((*string)(unsafe.Pointer(&buf)))
}

func Not(c Constraint) Constraint {
	return &notConstraint{c: c}
}

type notConstraint struct {
	c Constraint
}

func (nc *notConstraint) String() string {
	return fmt.Sprintf("not(%v)", nc.c.String())
}

func (nc *notConstraint) filter(ctx context.Context, rg parquet.RowGroup, primary bool, rr []rowRange) ([]rowRange, error) {
	base, err := nc.c.filter(ctx, rg, primary, rr)
	if err != nil {
		return nil, fmt.Errorf("unable to compute child constraint: %w", err)
	}
	// no need to intersect since its already subset of rr
	return complementRowRanges(base, rr), nil
}

func (nc *notConstraint) init(s *parquet.Schema) error {
	return nc.c.init(s)
}

func (nc *notConstraint) path() string {
	return nc.c.path()
}
