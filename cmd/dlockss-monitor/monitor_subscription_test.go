package main

import (
	"reflect"
	"testing"
)

func TestShardIDsUpToDepth(t *testing.T) {
	tests := []struct {
		depth int
		want  []string
	}{
		{-1, nil},
		{0, []string{""}},
		{1, []string{"", "0", "1"}},
		{2, []string{"", "0", "1", "00", "01", "10", "11"}},
		{3, []string{"", "0", "1", "00", "01", "10", "11", "000", "001", "010", "011", "100", "101", "110", "111"}},
	}
	for _, tt := range tests {
		got := shardIDsUpToDepth(tt.depth)
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("shardIDsUpToDepth(%d) = %v, want %v", tt.depth, got, tt.want)
		}
	}
}

func TestShardIDsUpToDepth_Count(t *testing.T) {
	for depth := 0; depth <= 6; depth++ {
		got := shardIDsUpToDepth(depth)
		want := 1<<(depth+1) - 1
		if len(got) != want {
			t.Errorf("shardIDsUpToDepth(%d) len = %d, want %d", depth, len(got), want)
		}
	}
}
