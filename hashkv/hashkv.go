package hashkv

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

type HashKV struct {
	logPath         string
	byteOffsetIndex map[string]int64
}

func (h *HashKV) Write(key string, value string) error {
	file, err := os.OpenFile(h.logPath, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening logPath: %v", err)
	}

	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	entry := []byte(fmt.Sprintf("{\"%s\":\"%s\"}", key, value))
	length := int64(len(entry))
	binary.Write(file, binary.LittleEndian, length)
	binary.Write(file, binary.LittleEndian, entry)

	h.byteOffsetIndex[key] = offset

	return nil
}

func (h *HashKV) Read(key string) (string, error) {
	offset, ok := h.byteOffsetIndex[key]
	if !ok {
		return "", nil
	}

	file, err := os.Open(h.logPath)
	if err != nil {
		return "", fmt.Errorf("error opening logPath: %v", err)
	}

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return "", fmt.Errorf("error seeking: %v", err)
	}

	length := int64(0)
	err = binary.Read(file, binary.LittleEndian, &length)
	if err != nil {
		return "", fmt.Errorf("error reading length: %v", err)
	}

	data := make([]byte, length)
	err = binary.Read(file, binary.LittleEndian, data)
	if err != nil {
		return "", fmt.Errorf("error reading data :%v", err)
	}

	return string(data), nil
}

// Delete is not yet implemented for HashKV
// TODO: Implement delete operations with tombstones for hashkv package
func (h *HashKV) Delete(key string) error {
	panic("Delete not yet implemented for HashKV")
}

func NewHashKV(logPath string) *HashKV {
	hashKV := &HashKV{
		logPath:         logPath,
		byteOffsetIndex: map[string]int64{},
	}

	_, err := os.Stat(logPath)
	if os.IsNotExist(err) {
		file, err := os.Create(logPath)
		if err != nil {
			log.Fatal("Failed to create file: ", err)
		}
		defer file.Close()
		return hashKV
	}

	// TODO: process the file if it exists, and fill the index
	file, err := os.Open(logPath)
	if err != nil {
		log.Fatal(err)
	}
	for {
		length := int64(0)
		pos, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		err = binary.Read(file, binary.LittleEndian, &length)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		data := make([]byte, length)
		err = binary.Read(file, binary.LittleEndian, &data)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		unmarshalled := make(map[string]any)
		err = json.Unmarshal(data, &unmarshalled)
		if err != nil {
			log.Fatal(err)
		}
		keys := make([]string, 0, len(unmarshalled))
		for k := range unmarshalled {
			keys = append(keys, k)
		}
		hashKV.byteOffsetIndex[keys[0]] = pos
	}

	return hashKV
}
