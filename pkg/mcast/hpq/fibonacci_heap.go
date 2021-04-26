package hpq

import (
	"container/list"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
)

type Heap interface {
	Insert(types.Message)

	Peek() interface{}

	Pop() interface{}

	Remove(types.Message) interface{}

	Values() []interface{}
}

type node struct {
	self     *list.Element
	parent   *node
	children *list.List
	marked   bool
	degree   uint
	position uint
	tag      interface{}
	content  types.Message
}

type FibonacciHeap struct {
	roots *list.List
	index map[interface{}]*node
	treeDegrees map[uint]*list.Element
	min *node
	size int
}

func NewHeap() Heap {
	return &FibonacciHeap{
		roots:       list.New(),
		index:       make(map[interface{}]*node),
		treeDegrees: make(map[uint]*list.Element),
		min:         nil,
		size:        0,
	}
}

func (n *node) less(other *node) bool {
	return n.content.HasHigherPriority(other.content)
}

func (f *FibonacciHeap) update(n *node, message types.Message) {
	n.content = message

	child := n.children.Front()
	for child != nil {
		childNode := child.Value.(*node)
		child = child.Next()
		if childNode.less(n) {
			f.cut(childNode)
			f.cascadingCut(n)
		}
	}

	if f.min == n {
		f.resetMin()
	}
}

func (f *FibonacciHeap) cut(n *node) {
	n.parent.children.Remove(n.self)
	n.parent.degree--
	n.parent = nil
	n.marked = false
	n.self = f.roots.PushBack(n)
}

func (f *FibonacciHeap) cascadingCut(n *node) {
	if n.parent != nil {
		if !n.marked {
			n.marked = true
		} else {
			parent := n.parent
			f.cut(n)
			f.cascadingCut(parent)
		}
	}
}

func (f *FibonacciHeap) resetMin() {
	f.min = f.roots.Front().Value.(*node)
	for tree := f.min.self.Next(); tree != nil; tree = tree.Next() {
		if tree.Value.(*node).less(f.min) {
			f.min = tree.Value.(*node)
		}
	}
}

func (f *FibonacciHeap) link(parent, child *node) {
	child.marked = false
	child.parent = parent
	child.self = parent.children.PushBack(child)
	parent.degree++
}

func (f *FibonacciHeap) consolidate() {
	for tree := f.roots.Front(); tree != nil; tree = tree.Next() {
		f.treeDegrees[tree.Value.(*node).position] = nil
	}

	for tree := f.roots.Front(); tree != nil; {
		if f.treeDegrees[tree.Value.(*node).degree] == nil {
			f.treeDegrees[tree.Value.(*node).degree] = tree
			tree.Value.(*node).position = tree.Value.(*node).degree
			tree = tree.Next()
			continue
		}

		if f.treeDegrees[tree.Value.(*node).degree] == tree {
			tree = tree.Next()
			continue
		}

		for f.treeDegrees[tree.Value.(*node).degree] != nil {
			another := f.treeDegrees[tree.Value.(*node).degree]
			f.treeDegrees[tree.Value.(*node).degree] = nil
			if tree.Value.(*node).less(another.Value.(*node)) {
				f.roots.Remove(another)
				f.link(tree.Value.(*node), another.Value.(*node))
			} else {
				f.roots.Remove(tree)
				f.link(another.Value.(*node), tree.Value.(*node))
				tree = another
			}
		}

		f.treeDegrees[tree.Value.(*node).degree] = tree
		tree.Value.(*node).position = tree.Value.(*node).degree
	}

	f.resetMin()
}

func (f *FibonacciHeap) extractMin() *node {
	min := f.min

	children := f.min.children
	if children != nil {
		for e := children.Front(); e != nil; e = e.Next() {
			e.Value.(*node).parent = nil
			e.Value.(*node).self = f.roots.PushBack(e.Value.(*node))
		}
	}

	f.roots.Remove(f.min.self)
	f.treeDegrees[min.position] = nil
	delete(f.index, f.min.tag)
	f.size--

	if f.size == 0 {
		f.min = nil
	} else {
		f.consolidate()
	}
	return min
}

func (f *FibonacciHeap) Insert(message types.Message) {
	if n, exists := f.index[message.Identifier]; exists {
		f.update(n, message)
		return
	}

	n := new(node)
	n.children = list.New()
	n.tag = message.Identifier
	n.content = message

	n.self = f.roots.PushBack(n)
	f.index[n.tag] = n
	f.size++

	if f.min == nil || n.less(f.min) {
		f.min = n
	}
}

func (f *FibonacciHeap) Pop() interface{} {
	if f.size == 0 {
		return nil
	}

	n := f.extractMin()
	return n.content
}

func (f *FibonacciHeap) Peek() interface{} {
	if f.min == nil {
		return nil
	}
	return f.min.content
}

func (f *FibonacciHeap) Remove(message types.Message) interface{} {
	n, exists := f.index[message.Identifier]
	if !exists {
		return nil
	}

	if n.parent != nil {
		parent := n.parent
		f.cut(n)
		f.cascadingCut(parent)
	}

	if n.parent == nil {
		f.min = n
	}

	return f.Pop()
}

func (f *FibonacciHeap) Values() []interface{} {
	var values []interface{}
	for r := f.roots.Front(); r != nil; r = r.Next() {
		for n := r.Value.(*node).children.Front(); n != nil; n = n.Next() {
			values = append(values, n.Value.(*node).content)
		}
	}
	return values
}



