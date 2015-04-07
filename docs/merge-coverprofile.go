package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {

	modeline := ""
	blocks := map[string]int{}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "mode:") {
			lastSpace := strings.LastIndex(line, " ")
			prefix := line[0:lastSpace]
			suffix := line[lastSpace+1:]
			count, err := strconv.Atoi(suffix)
			if err != nil {
				fmt.Printf("error parsing count: %v", err)
				continue
			}
			existingCount, exists := blocks[prefix]
			if exists {
				blocks[prefix] = existingCount + count
			} else {
				blocks[prefix] = count
			}
		} else if modeline == "" {
			modeline = line
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}

	fmt.Println(modeline)
	for k, v := range blocks {
		fmt.Printf("%s %d\n", k, v)
	}
}
