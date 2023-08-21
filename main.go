package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

type StatementType int64
type PrepareResult int64

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT

	PREPARE_SUCCESS PrepareResult = iota
	PREPARE_UNRECOGNIZED
	PREPARE_SYNTAX_ERROR

	// size in bytes
	ID_SIZE       = 4
	USERNAME_SIZE = 32
	EMAIL_SIZE    = 255
	ROW_SIZE      = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

	// offsets
	ID_OFFSET       = 0
	USERNAME_OFFSET = ID_SIZE + ID_OFFSET
	EMAIL_OFFSET    = USERNAME_OFFSET + USERNAME_SIZE

	// page
	PAGE_SIZE     = 4096
	ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE

	TABLE_MAX_PAGES = 100
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

type Pager struct {
	fileLength int
	pages      []byte
}

type Table struct {
	// pager *Pager
	rowCount int
	pages    [][]byte
}

func (t *Table) rowSlot(rowNum int) *[]byte {
	pageNum := rowNum / ROWS_PER_PAGE
	if rowNum >= TABLE_MAX_ROWS {
		return nil
	}
	page := t.pages[pageNum]
	rowOffset := rowNum % ROWS_PER_PAGE
	byteOffset := rowOffset * ROW_SIZE
	pageOffset := page[byteOffset:]
	return &pageOffset
}

func serializeRow(row Row, pages *[]byte) {
	binary.LittleEndian.PutUint32((*pages)[ID_OFFSET:ID_OFFSET+ID_SIZE], row.id)
	copy((*pages)[USERNAME_OFFSET:USERNAME_OFFSET+USERNAME_SIZE], []byte(row.username))
	copy((*pages)[EMAIL_OFFSET:EMAIL_OFFSET+EMAIL_SIZE], []byte(row.email))
}

func deserializeRow(pages *[]byte) (row Row) {
	row.id = binary.LittleEndian.Uint32((*pages)[ID_OFFSET : ID_OFFSET+ID_SIZE])
	row.username = string((*pages)[USERNAME_OFFSET : USERNAME_OFFSET+USERNAME_SIZE])
	row.email = string((*pages)[EMAIL_OFFSET : EMAIL_OFFSET+EMAIL_SIZE])
	return
}

type Row struct {
	id       uint32
	username string
	email    string
}

func pagerOpen(fileName string) (*Pager, error) {
	_f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer _f.Close()

	if bytes, err := io.ReadAll(_f); err != nil {
		return nil, err
	} else {
		p := Pager{
			pages:      bytes,
			fileLength: len(bytes),
		}

		return &p, nil
	}
}

func dbOpen(fileName string) (*Table, error) {
	// pager, err := pagerOpen(fileName)
	// if err != nil {
	// 	return nil, err
	// }

	// t := Table{
	// 	pager: pager,
	// }
	table := Table{
		rowCount: 0,
		pages:    make([][]byte, TABLE_MAX_ROWS),
	}
	for i := range table.pages {
		table.pages[i] = make([]byte, PAGE_SIZE)
	}

	return &table, nil
}

func printPrompt() {
	fmt.Print("db >")
}

func readInput(reader *bufio.Reader) (string, error) {
	if readed, err := reader.ReadString('\n'); err != nil {
		return "", err
	} else {
		return strings.TrimSuffix(readed, "\n"), nil
	}
}

/*
Interface
*/
func doMetaCommand(input string) bool {
	if strings.Compare(input, ".exit") == 0 {
		fmt.Println("Exit program. Bye")
		os.Exit(0)
	} else {
		return false
	}
	return true
}

func executeInsert(input string, table *Table) {
	row := Row{}

	if _, err := fmt.Sscanf(input, "insert %d %s %s", &row.id, &row.username, &row.email); err != nil {
		fmt.Println(err)
		fmt.Printf("Syntax error. cannot parse query %s\n", input)
		return
	}

	serializeRow(row, table.rowSlot(table.rowCount))
	table.rowCount += 1
}

func executeSelect(table *Table) {

	for i := 0; i < table.rowCount; i++ {
		row := deserializeRow(table.rowSlot(i))
		fmt.Printf("Row %d: %+v\n", i, row)
	}

}

func main() {

	reader := bufio.NewReader(os.Stdin)

	table, err := dbOpen("")
	if err != nil {
		os.Exit(1)
	}
	for {
		printPrompt()

		input, err := readInput(reader)
		if err != nil {
			fmt.Println("An error occured while reading input. Please try again", err)
			continue
		} else if len(input) == 0 {
			continue
		}

		if input[0] == '.' {
			if !doMetaCommand(input) {
				fmt.Printf("Unrecognized command %s \n", input)
				continue
			}
		}

		if strings.HasPrefix(input, "insert ") {
			executeInsert(input, table)
		} else if strings.HasPrefix(input, "select ") {
			executeSelect(table)
		} else {
			fmt.Printf("Unrecognized keyword at start of %s \n", input)
			continue
		}
		fmt.Println("Executed.")
	}
}
