package errors

import "errors"

type MultiError struct {
	msg    string
	errors []error
}

func NewMultiError(msg string) *MultiError {
	return &MultiError{
		msg: msg,
	}
}

func (m *MultiError) Append(err error) {
	if err != nil {
		m.errors = append(m.errors, err)
	}
}

func IsEmptyError(err error) bool {
	var me *MultiError
	if errors.As(err, &me) {
		return len(me.errors) == 0
	}
	return false
}

func (m *MultiError) Error() string {
	errStr := m.msg + ": "
	for _, err := range m.errors {
		errStr = errStr + ": " + err.Error() + "\n"
	}
	return errStr
}
