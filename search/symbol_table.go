// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"slices"

	"github.com/parquet-go/parquet-go"
)

// symbolTable is a helper that can decode the i-th value of a page.
// Using it we only need to allocate an int32 slice and not a slice of
// string values.
// It only works for optional dictionary encoded columns. All of our label
// columns are that though.
type symbolTable struct {
	dict parquet.Dictionary
	syms []int32
}

func (s *symbolTable) Get(i int) parquet.Value {
	switch s.syms[i] {
	case -1:
		return parquet.NullValue()
	default:
		return s.dict.Index(s.syms[i])
	}
}

func (s *symbolTable) GetIndex(i int) int32 {
	return s.syms[i]
}

func (s *symbolTable) Reset(pg parquet.Page) {
	dict := pg.Dictionary()
	data := pg.Data()
	syms := data.Int32()
	defs := pg.DefinitionLevels()

	if s.syms == nil {
		s.syms = make([]int32, len(defs))
	} else {
		s.syms = slices.Grow(s.syms, len(defs))[:len(defs)]
	}

	sidx := 0
	for i := range defs {
		if defs[i] == 1 {
			s.syms[i] = syms[sidx]
			sidx++
		} else {
			s.syms[i] = -1
		}
	}
	s.dict = dict
}
