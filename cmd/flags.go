// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package main

import (
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"gopkg.in/alecthomas/kingpin.v2"
)

type matcherSlice []*labels.Matcher

func (a *matcherSlice) Set(value string) error {
	matchers, err := parser.ParseMetricSelector(value)
	if err != nil {
		return err
	}
	*a = append(*a, matchers...)

	return nil
}

func (a *matcherSlice) String() string {
	var b strings.Builder
	for i, m := range *a {
		b.WriteString(m.String())
		if i != len(*a) {
			b.WriteString(",")
		}
	}
	return b.String()
}

func MatchersVar(flags *kingpin.FlagClause, target *matcherSlice) {
	flags.SetValue((*matcherSlice)(target))
	return
}
