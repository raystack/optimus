// Code generated by mockery v2.10.6. DO NOT EDIT.

package mock

import mock "github.com/stretchr/testify/mock"

// AssetOperator is an autogenerated mock type for the AssetOperator type
type AssetOperator struct {
	mock.Mock
}

// Install provides a mock function with given fields: asset, tagName
func (_m *AssetOperator) Install(asset []byte, tagName string) error {
	ret := _m.Called(asset, tagName)

	var r0 error
	if rf, ok := ret.Get(0).(func([]byte, string) error); ok {
		r0 = rf(asset, tagName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Prepare provides a mock function with given fields: localDirPath
func (_m *AssetOperator) Prepare(localDirPath string) error {
	ret := _m.Called(localDirPath)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(localDirPath)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Run provides a mock function with given fields: tagName, args
func (_m *AssetOperator) Run(tagName string, args ...string) error {
	_va := make([]interface{}, len(args))
	for _i := range args {
		_va[_i] = args[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, tagName)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, ...string) error); ok {
		r0 = rf(tagName, args...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Uninstall provides a mock function with given fields: tagNames
func (_m *AssetOperator) Uninstall(tagNames ...string) error {
	_va := make([]interface{}, len(tagNames))
	for _i := range tagNames {
		_va[_i] = tagNames[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 error
	if rf, ok := ret.Get(0).(func(...string) error); ok {
		r0 = rf(tagNames...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}