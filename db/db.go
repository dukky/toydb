package db

type DB interface {
	Write(string, string) error
	Read(string) (string, error)
}