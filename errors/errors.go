package errors

import "fmt"

type PropertyNotFoundError struct {
	PropertyName string
}

func (e PropertyNotFoundError) Error() string {
	return fmt.Sprintf("Property %v not found", e.PropertyName)
}

type ListIndexExceedsLengthError struct {
	Index int
}

func (e ListIndexExceedsLengthError) Error() string {
	return fmt.Sprintf("List index %v exceeds length", e.Index)
}

type InvalidOperationError struct {
	Reason string
}

func (e InvalidOperationError) Error() string {
	return fmt.Sprintf("Invalid operation: %v", e.Reason)
}

type UnknownError struct {
}

func (e UnknownError) Error() string {
	return fmt.Sprintf("Unknown error")
}
