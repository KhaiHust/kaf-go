package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const metaFileName = "meta.json"

type TopicMeta struct {
	Name              string `json:"name"`
	TopicId           string `json:"topicId"`
	NumPartitions     int32  `json:"numPartitions"`
	ReplicationFactor int16  `json:"replicationFactor"`
	MinInsyncReplicas int16  `json:"minInsyncReplicas" default:"1"`
}

func SaveTopicMeta(topicDir string, meta TopicMeta) error {
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return fmt.Errorf("failed to create topic dir: %w", err)
	}
	path := filepath.Join(topicDir, metaFileName)
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func LoadTopicMeta(topicDir string) (*TopicMeta, error) {
	path := filepath.Join(topicDir, metaFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var meta TopicMeta
	return &meta, json.Unmarshal(data, &meta)
}
