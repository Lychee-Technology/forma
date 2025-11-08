package internal

import (
	"strings"
	"text/template"
)

func renderTemplate(tpl *template.Template, data any) (string, error) {
	var builder strings.Builder
	if err := tpl.Execute(&builder, data); err != nil {
		return "", err
	}
	return builder.String(), nil
}
