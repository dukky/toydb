package log

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

type Log struct {
	LogPath string
	Index   map[string]int64
}

type Entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (l *Log) Write(key string, value string) error {
	file, err := os.OpenFile(l.LogPath, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	marshalled, err := json.Marshal(Entry{
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("error marshalling json: %v", err)
	}

	marshalled = append(marshalled, '\n')
	_, err = file.Write(marshalled)
	if err != nil {
		return fmt.Errorf("error writing data: %v", err)
	}

	l.Index[key] = offset

	return nil
}

func (l *Log) Read(key string) (string, error) {
	offset, ok := l.Index[key]
	if !ok {
		return "", nil
	}
	file, err := os.Open(l.LogPath)
	if err != nil {
		return "", fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return "", fmt.Errorf("error seeking: %v", err)
	}
	reader := bufio.NewReader(file)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("error reading line: %v", err)
	}
	var entry Entry
	err = json.Unmarshal([]byte(line), &entry)
	if err != nil {
		return "", fmt.Errorf("error unmarshalling json: %v", err)
	}
	return entry.Value, nil
}

func NewLog(logPath string) *Log {
	db := &Log{
		LogPath: logPath,
		Index:   make(map[string]int64),
	}
	_, err := os.Stat(logPath)
	if os.IsNotExist(err) {
		file, err := os.Create(logPath)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		return db
	}
	if err = db.compact(); err != nil {
		log.Fatal(err)
	}
	if err = db.initIndex(); err != nil {
		log.Fatal(err)
	}
	return db
}

func (l *Log) compact() error {
	readFile, err := os.Open(l.LogPath)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer readFile.Close()
	scanner := bufio.NewScanner(readFile)
	seen := make(map[string]string)
	for scanner.Scan() {
		var entry Entry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return fmt.Errorf("error unmarshalling json: %v", err)
		}
		seen[entry.Key] = entry.Value
	}
	tmpFile, err := os.Create(l.LogPath + ".tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()
	for k, v := range seen {
		entry, err := json.Marshal(Entry{
			Key:   k,
			Value: v,
		})
		if err != nil {
			return fmt.Errorf("error marshalling json: %v", err)
		}
		entry = append(entry, '\n')
		_, err = tmpFile.Write(entry)
		if err != nil {
			return fmt.Errorf("error writing data: %v", err)
		}
	}
	err = os.Rename(tmpFile.Name(), l.LogPath)
	if err != nil {
		return fmt.Errorf("error renaming file: %v", err)
	}
	return nil
}

func (l *Log) initIndex() error {
	l.Index = make(map[string]int64)
	file, err := os.Open(l.LogPath)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()
	var offset int64 = 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry Entry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return fmt.Errorf("error unmarshalling json: %v", err)
		}
		l.Index[entry.Key] = offset
		offset += int64(len(scanner.Bytes()) + 1) // +1 byte for /n consumed by scanner
	}
	return nil
}
