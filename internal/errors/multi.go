package errors

import "errors"

type MultiError struct {
	msg    string
	Errors []error
}

func NewMultiError(msg string) *MultiError {
	return &MultiError{
		msg: msg,
	}
}

func (m *MultiError) Append(err error) {
	if err == nil {
		return
	}

	var me *MultiError
	if errors.As(err, &me) { // Flatten the multi error
		m.Errors = append(m.Errors, me.Errors...)
		return
	}

	m.Errors = append(m.Errors, err)
}

func (m *MultiError) Error() string {
	errStr := m.msg
	for _, err := range m.Errors {
		errStr = errStr + ":\n " + err.Error()
	}
	return errStr
}

func MultiToError(e error) error {
	var me *MultiError
	if errors.As(e, &me) {
		if len(me.Errors) == 0 {
			return nil
		}
		return me
	}
	return e
}
