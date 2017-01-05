package drv

import (
	"fmt"
	"strings"
)

// SQLtext and SQLpart are convenience objects, that you can use to easily create the SQL text string of a batch.
//
// A SQLtext object is just an aggregator for SQLpart objects.
// If you just have one SQLpart object, you don't need to use a SQLtext object.
//
type SQLtext struct {
	parts    []*SQLpart
	linefeed []bool
}

// NewSQLtext returns a new SQLtext object.
//
// Then, you will construct the batch text by adding SQLPart objects to SQLtext, with the methods Addln and Add.
// Finally, you call the Text method to retrieve the resulting string.
//
func NewSQLtext() *SQLtext {

	return &SQLtext{}
}

// Addln appends a SQLpart to an SQLtext object, and adds a linefeed '\n' character at the end.
//
func (sqltext *SQLtext) Addln(part *SQLpart) {

	sqltext.parts = append(sqltext.parts, part)
	sqltext.linefeed = append(sqltext.linefeed, true)
}

// Addln appends a SQLpart to an SQLtext object.
//
func (sqltext *SQLtext) Add(part *SQLpart) {

	sqltext.parts = append(sqltext.parts, part)
	sqltext.linefeed = append(sqltext.linefeed, false)
}

// PartCount returns the number of parts in the SQLtext object.
//
func (sqltext *SQLtext) PartCount() int {

	return len(sqltext.parts)
}

// Part returns the SQLpart in SQLtext at index i.
//
func (sqltext *SQLtext) Part(i int) *SQLpart {

	return sqltext.parts[i]
}

// Text returns the concatenation of all SQLpart strings it contains, sequentially.
// All placeholders in SQLparts text are replaced by the values set by BindStr, BindInt, etc methods.
//
// If a placeholder has not been replaced by a value or any other error occurred during a BindStr, etc method, an error is returned.
//
func (sqltext *SQLtext) Text() (string, error) {
	var (
		err      error
		buff     []byte
		partText string
	)

	buff = make([]byte, 0, 100)

	for i, part := range sqltext.parts {
		if part.err != nil {
			return "", part.err
		}

		if partText, err = part.Text(); err != nil {
			return "", err
		}

		buff = append(buff, partText...)

		if sqltext.linefeed[i] {
			buff = append(buff, '\n')
		}
	}

	return string(buff), nil
}

// SQLpart contains a part of the SQL text of a batch or the full SQL text.
// It is created by the NewSQLpart() function, and can contain named placeholders, which will be filled by BindStr, BindInt, etc methods.
//
type SQLpart struct {
	text           string           // original SQL text
	textFragments  []interface{}    // string for sql text parts, and nil for placeholders
	placeholderMap map[string][]int // for each placeholder, value is the list of indices in textFragments slice referencing the placeholder name

	err error // if error occured during a BindString, BindInt, etc operation
}

// Err returns an error if a BindStr, BindInt, etc operation on SQLpart has failed.
// Instead of checking error after each Bind method, it is easier to just check after all operations on SQLpart have been performed.
//
func (part *SQLpart) Err() error {

	return part.err
}

// NewSQLpart creates a SQLpart object, containing the specified SQL text.
//
// The SQL text can contain many lines and many SQL statements.
// It can also contain named placeholders, specified between {{ and }}, e.g.:
//
//     SELECT * FROM
//     employees WHERE lastname = {{name}}
//
// A placeholder name is case insensitive. They are replaced by values by the functions BindString, BindInt, etc.
// Many placeholders can have the same name, and will all be replaced by the same value.
//
// By default, placeholder delimiters are {{ and }}, but you can pass other opening and closing delimiters as two optional arguments.
//
// If incorrect syntax is found with placeholder or delimiters in text argument (e.g. missing closing delimiter), the function panics.
//
// Example:
//
//    p := drv.NewSQLpart("INSERT INTO mydb..parents (firstName, lastName) VALUES ({{fname}}, {{lname}});")
//    p.BindStr("fname", "John").BindStr("lname", "O'Hara")
//    s, err := p.Text()
//    if err != nil {
//        log.Fatalf("%s", err)
//    }
//    fmt.Println(s)  // prints     INSERT INTO mydb..parents (firstName, lastName) VALUES ('John', 'O''Hara');
//
func NewSQLpart(text string, placeholderDelimiters ...string) *SQLpart {
	type State uint8

	const (
		StateText State = iota
		StatePlaceholder
	)

	var (
		delimLeft        string = "{{"
		delimLeftLength  int
		delimRight       string = "}}"
		delimRightLength int

		sqlpart           *SQLpart
		textLength        int
		lineNo            int
		textFragmentStart int
		placeholderStart  int
		state             State
		textFragments     []interface{}    // string for sql text parts, and nil for placeholders
		placeholderMap    map[string][]int // for each placeholder, value is the list of indices in textFragments slice referencing the placeholder name
	)

	sqlpart = &SQLpart{}

	// define delimiters for placeholders

	if placeholderDelimiters != nil {
		if len(placeholderDelimiters) != 2 {
			panic("SQLpart: opening and terminating delimiters must be provided.")
		}

		delimLeft = placeholderDelimiters[0]
		delimRight = placeholderDelimiters[1]
	}

	delimLeftLength = len(delimLeft)
	delimRightLength = len(delimRight)

	if delimLeftLength == 0 {
		panic("SQLpart: opening delimiter for placeholder cannot be empty string.")
	}

	if delimRightLength == 0 {
		panic("SQLpart: terminating delimiter for placeholder cannot be empty string.")
	}

	if delimLeft == delimRight {
		panic("SQLpart: opening and terminating delimiters for placeholder must be different.")
	}

	// parse the sql text and split it at placeholder positions

	sqlpart.text = text

	textLength = len(text)
	textFragmentStart = 0
	placeholderStart = -1
	state = StateText
	lineNo = 1

	i := 0
	for i < textLength {
		if i+delimLeftLength <= textLength && text[i:i+delimLeftLength] == delimLeft {
			if state != StateText {
				panic(fmt.Sprintf("SQLpart: invalid opening delimiter for placeholder (line %d).", lineNo))
			}
			state = StatePlaceholder

			if textFragmentStart != i {
				textFragments = append(textFragments, text[textFragmentStart:i])
			}

			i += delimLeftLength
			textFragmentStart = -1
			placeholderStart = i

			continue
		}

		if i+delimRightLength <= textLength && text[i:i+delimRightLength] == delimRight {
			if state != StatePlaceholder {
				panic(fmt.Sprintf("SQLpart: invalid terminating delimiter for placeholder (line %d).", lineNo))
			}

			placeholderEndx := i
			placeholderName := strings.TrimSpace(strings.ToLower(text[placeholderStart:placeholderEndx]))

			if len(placeholderName) == 0 {
				panic(fmt.Sprintf("SQLpart: placeholder name cannot be empty (line %d).", lineNo))
			}

			textFragments = append(textFragments, nil) // the Bindxxx functions will replace these strings by parameter values

			if placeholderMap == nil {
				placeholderMap = make(map[string][]int)
			}

			pos := len(textFragments) - 1
			placeholderMap[placeholderName] = append(placeholderMap[placeholderName], pos)

			i += delimRightLength
			textFragmentStart = i
			state = StateText

			continue
		}

		if text[i] == '\n' {
			if state == StatePlaceholder {
				panic(fmt.Sprintf("SQLpart: placeholder closing delimiter not found (line %d).", lineNo))
			}
			lineNo++
		}

		i++
	}

	if state != StateText {
		panic(fmt.Sprintf("SQLpart: terminating delimiter expected for placeholder (line %d).", lineNo))
	}

	if textFragmentStart != i {
		textFragments = append(textFragments, text[textFragmentStart:i])
	}

	if false { // for debugging
		for _, s := range textFragments {
			ss := "{{}}"
			if s != nil {
				ss = s.(string)
			}

			fmt.Println("<" + ss + ">")
		}
	}

	sqlpart.textFragments = textFragments
	sqlpart.placeholderMap = placeholderMap

	return sqlpart
}

// Text returns the SQL text, with the placeholders replaced by the values specified by BindString, BindInt, etc functions.
// If all placeholders have not been replaced by a value, an error is returned.
//
// Like the Err method, Text returns an error if a BindStr, BindInt, etc operation on SQLpart has failed. It also returns an error if all placeholders have not been replaced by a value.
//
func (part *SQLpart) Text() (string, error) {
	var buff []byte

	if part.err != nil {
		return "", part.err
	}

	buff = make([]byte, 0, 100)

	for i, fragment := range part.textFragments { // for each fragment of the SQL text
		if fragment == nil { // if the fragment is a placeholder which has not been replaced by a value
			for name, targets := range part.placeholderMap { // lookup for the placeholder name pointing to this position
				for _, k := range targets {
					if i == k {
						return "", fmt.Errorf("SQL text: placeholder \"%s\" has not been filled by a Bind method.", name) // and return error
					}
				}
			}
			panic("placeholder position not referenced in placeholderMap")
		}

		buff = append(buff, fragment.(string)...)
	}

	return string(buff), nil
}
