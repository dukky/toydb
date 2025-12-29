package logdb

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
)

type Log struct {
	LogPath string
}

type Entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (l *Log) Write(key string, value string) error {
	file, err := os.OpenFile(l.LogPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()
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

	return nil
}

func (l *Log) Read(key string) (string, error) {
	file, err := os.Open(l.LogPath)
	if err != nil {
		return "", fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	latest := ""
	for scanner.Scan() {
		var entry Entry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return "", fmt.Errorf("error unmarshalling json: %v", err)
		}
		if entry.Key == key {
			latest = entry.Value
		}
	}
	return latest, nil
}

func NewLog(logPath string) *Log {
	db := &Log{
		LogPath: logPath,
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
	if err = compact(db); err != nil {
		log.Fatal(err)
	}
	return db
}

func compact(l *Log) error {
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
	writeFile, err := os.Create(l.LogPath)
	if err != nil {
		return err
	}
	defer writeFile.Close()
	for k, v := range seen {
		entry, err := json.Marshal(Entry{
			Key:   k,
			Value: v,
		})
		if err != nil {
			return fmt.Errorf("error marshalling json: %v", err)
		}
		entry = append(entry, '\n')
		_, err = writeFile.Write(entry)
		if err != nil {
			return fmt.Errorf("error writing data: %v", err)
		}
	}
	return nil
}
