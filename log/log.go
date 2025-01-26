package logdb

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
)

type Log struct {
	LogPath string
}

func (l *Log) Write(key string, value string) error {
	file, err := os.OpenFile(l.LogPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	marshalled, err := json.Marshal(map[string]string{
		key: value,
	})
	if err != nil {
		return fmt.Errorf("error marshalling json: %v", err)
	}
	_, err = file.Write(marshalled)
	if err != nil {
		return fmt.Errorf("error writing data: %v", err)
	}
	_, err = file.Write([]byte("\n"))
	if err != nil {
		return fmt.Errorf("error writing newline: %v", err)
	}

	return nil
}

func (l *Log) Read(key string) (string, error) {
	file, err := os.Open(l.LogPath)
	if err != nil {
		return "", fmt.Errorf("error opening file: %v", err)
	}
	scanner := bufio.NewScanner(file)
	latest := ""
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "{\""+key+"\":") {
			latest = strings.TrimSuffix(strings.TrimPrefix(line, "{\""+key+"\":\""), "\"}")
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
	file, err := os.Open(l.LogPath)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	scanner := bufio.NewScanner(file)
	seen := make(map[string]struct{})
	for scanner.Scan() {
		seen[scanner.Text()] = struct{}{}
	}
	file.Close()
	file, err = os.Create(l.LogPath)
	if err != nil {
		return err
	}
	fmt.Println(seen)
	for k := range seen {
		data, err := json.Marshal(k)
		if err != nil {
			return fmt.Errorf("error marshalling line: %v", err)
		}
		_, err = file.Write(data)
		if err != nil {
			return fmt.Errorf("error writing data: %v", err)
		}
		_, err = file.Write([]byte("\n"))
		if err != nil {
			return fmt.Errorf("error writing newline: %v", err)
		}
	}
	return nil
}
