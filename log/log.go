package logdb

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
	// TODO: This compact function has a bug!
	// Currently it deduplicates entire lines (seen[scanner.Text()]), but it should
	// deduplicate by KEY, keeping only the latest VALUE for each key.
	//
	// The issue: If you write key="foo" value="bar", then write key="foo" value="baz",
	// both lines are kept because they're different strings. But compact should only
	// keep the latest entry: key="foo" value="baz".
	//
	// Hint: You need to parse the JSON on each line to extract the key-value pairs,
	// and store them in a map[string]string instead of map[string]struct{}.
	// See TestCompactDuplicateKeys in log_test.go for a failing test that demonstrates this bug.

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
		var data bytes.Buffer
		encoder := json.NewEncoder(&data)
		encoder.SetEscapeHTML(false)
		err := encoder.Encode(k)
		if err != nil {
			return fmt.Errorf("error marshalling line: %v", err)
		}
		_, err = io.Copy(file, &data)
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
