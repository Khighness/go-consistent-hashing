package core

import "errors"

// @Author KHighness
// @Update 2022-06-24

var (
	ErrHostAlreadyExists = errors.New("host already exists")
	ErrHostNotFound      = errors.New("host not found")
)

