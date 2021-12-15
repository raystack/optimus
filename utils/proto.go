package utils

import (
	"strings"
)

// ToEnumProto converts models to Types defined in protobuf, task -> TYPE_TASK
func ToEnumProto(modelType string, enumName string) string {
	return strings.ToUpper(enumName + "_" + modelType)
}

// FromEnumProto converts models to Types defined in protobuf, TYPE_TASK -> task
func FromEnumProto(typeProto string, enumName string) string {
	return strings.TrimPrefix(strings.ToLower(typeProto), strings.ToLower(enumName+"_"))
}
