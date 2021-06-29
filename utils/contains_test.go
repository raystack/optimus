package utils

import "testing"

func TestContainsString(t *testing.T) {
	type args struct {
		s []string
		v string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "should found an item if present",
			args: args{
				s: []string{"a", "abc", "d"},
				v: "abc",
			},
			want: true,
		},
		{
			name: "should not found an item if not present",
			args: args{
				s: []string{"a", "abc", "d"},
				v: "abcd",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ContainsString(tt.args.s, tt.args.v); got != tt.want {
				t.Errorf("ContainsString() = %v, want %v", got, tt.want)
			}
		})
	}
}
