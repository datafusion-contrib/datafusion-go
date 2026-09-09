package conformance

import (
	"testing"
	"testing/fstest"
)

func TestCorpusRejectsInvalidFixtures(t *testing.T) {
	for name, text := range map[string]string{
		"empty":             `[]`,
		"unknown field":     `[{"name":"a","sql":"select 1","unexpected":true}]`,
		"missing name":      `[{"sql":"select 1"}]`,
		"missing sql":       `[{"name":"a"}]`,
		"duplicate name":    `[{"name":"a","sql":"select 1"},{"name":"a","sql":"select 2"}]`,
		"wrong width":       `[{"name":"a","sql":"select 1","rows":[["1"]]}]`,
		"trailing document": `[{"name":"a","sql":"select 1"}] []`,
		"trailing garbage":  `[{"name":"a","sql":"select 1"}] bad`,
		"malformed":         `[`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(fstest.MapFS{"case.json": &fstest.MapFile{Data: []byte(text)}}); err == nil {
				t.Fatal("accepted invalid corpus")
			}
		})
	}
	if _, err := Load(fstest.MapFS{}); err == nil {
		t.Fatal("accepted an empty corpus directory")
	}
}

func TestCorpusRetainsNullAndLargeInteger(t *testing.T) {
	text := `[{"name":"values","sql":"select 1","columns":[{"name":"n","type":"int64","nullable":true}],"rows":[[null],["9223372036854775807"]]}]`
	cases, err := Load(fstest.MapFS{"case.json": &fstest.MapFile{Data: []byte(text)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) != 1 || len(cases[0].Rows) != 2 || cases[0].Rows[0][0] != nil || *cases[0].Rows[1][0] != "9223372036854775807" {
		t.Fatalf("unexpected decoded values: %+v", cases)
	}
}
