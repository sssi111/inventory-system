package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"time"
)

type Config struct {
	Host      string
	Port      int
	Password  string
	Directory string
}

func (c *Config) Load() error {
	file, err := os.Open("config.json")
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	config := Config{}
	err = decoder.Decode(&config)
	if err != nil {
		return err
	}
	return nil
}

func WatchDirectory(dir string, queue chan string, done chan bool) {
	watchedFiles := make(map[string]bool)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			files, err := os.ReadDir(dir)
			if err != nil {
				log.Println("Error:", err)
				continue
			}
			for _, file := range files {
				if filepath.Ext(file.Name()) != ".tsv" || watchedFiles[file.Name()] {
					continue
				}
				watchedFiles[file.Name()] = true
				queue <- filepath.Join(dir, file.Name())
			}
		}
	}
}

func ProcessFiles(queue chan string, handler Handler) {
	for {
		file := <-queue
		err := handler.Handle(file)
		if err != nil {
			log.Println("Error processing file", file, ":", err)
		}
	}
}

func main() {
	var config Config

	if err := config.Load(); err != nil {
		log.Fatal("Unable to load config:", err)
	}

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d password=%s", config.Host, config.Port, config.Password))
	if err != nil {
		log.Fatal("Unable to connect to database:", err)
	}

	handler := &MyHandler{db: db, outputDirPath: "./output_files"}

	queue := make(chan string, 100)
	done := make(chan bool)

	go WatchDirectory(config.Directory, queue, done)
	go ProcessFiles(queue, handler)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	select {
	case <-signals:
		done <- true
	}
}
