/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package integration

import (
	"strings"
)

// ast type helpers

func sliceStringAST(els ...AST) string {
	result := make([]string, len(els))
	for i, el := range els {
		result[i] = el.String()
	}
	return strings.Join(result, ", ")
}
func sliceStringLeaf(els ...*Leaf) string {
	result := make([]string, len(els))
	for i, el := range els {
		result[i] = el.String()
	}
	return strings.Join(result, ", ")
}

// the methods below are what the generated code expected to be there in the package

type ApplyFunc func(*Cursor) bool

type Cursor struct {
	parent   AST
	replacer replacerFunc
	node     AST
}

// Node returns the current Node.
func (c *Cursor) Node() AST { return c.node }

// Parent returns the parent of the current Node.
func (c *Cursor) Parent() AST { return c.parent }

// Replace replaces the current node in the parent field with this new object. The use needs to make sure to not
// replace the object with something of the wrong type, or the visitor will panic.
func (c *Cursor) Replace(newNode AST) {
	c.replacer(newNode, c.parent)
	c.node = newNode
}

type replacerFunc func(newNode, parent AST)

func Rewrite(node AST, pre, post ApplyFunc) (AST, error) {
	outer := &struct{ AST }{node}

	if pre == nil {
		pre = func(cursor *Cursor) bool {
			return true
		}
	}

	if post == nil {
		post = func(cursor *Cursor) bool {
			return true
		}
	}

	err := rewriteAST(outer, node, func(newNode, parent AST) {
		outer.AST = newNode
	}, pre, post)

	if err != nil {
		return nil, err
	}
	return outer.AST, nil
}
