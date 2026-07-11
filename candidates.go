package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
)

var delimiters = [...]string{"-", "_", ".", ""}

type submitFunc func(context.Context, string) error

func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

func produceCandidates(ctx context.Context, names []string, wordlist string, suffixes []string, submit submitFunc) error {
	for _, name := range names {
		if err := submit(ctx, name); err != nil {
			return err
		}
	}

	file, err := os.Open(wordlist)
	if err != nil {
		return fmt.Errorf("open wordlist: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		word := scanner.Text()
		for _, name := range names {
			for _, delimiter := range delimiters {
				first := name + delimiter + word
				second := word + delimiter + name

				if err := submit(ctx, first); err != nil {
					return err
				}
				if err := submit(ctx, second); err != nil {
					return err
				}

				for _, suffix := range suffixes {
					candidates := [...]string{
						first + delimiter + suffix,
						suffix + delimiter + first,
						second + delimiter + suffix,
						suffix + delimiter + second,
					}
					for _, candidate := range candidates {
						if err := submit(ctx, candidate); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read wordlist: %w", err)
	}
	return nil
}
