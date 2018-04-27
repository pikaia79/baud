// Implementation of an R-Way Trie data structure.
//
// A Trie has a root Node which is the base of the tree.
package triedb

import (
	"sort"
	"fmt"

	"github.com/tiglabs/baudengine/util/bufalloc"
)

type NodeIterator func(key []byte, val interface{}) bool

type Node struct {
	val      byte         // value of node
	term     bool         // last node flag
	depth    int
	property interface{}  // property of node
	parent   *Node
	children map[byte]*Node
}

type Trie struct {
	root *Node
	size int
}

type ByBytes []byte
func (a ByBytes) Len() int           { return len(a) }
func (a ByBytes) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByBytes) Less(i, j int) bool { return a[i] < a[j] }

// Creates a new Trie with an initialized root Node.
func NewTrie() *Trie {
	return &Trie{
		root: &Node{children: make(map[byte]*Node), depth: 0},
		size: 0,
	}
}

// Returns the root node for the Trie.
func (t *Trie) Root() *Node {
	return t.root
}

func (t *Trie) Size() int {
	return t.size
}

// ReplaceOrInsert adds the given key to the tree.  If an key in the tree
// already equals the given one, it is removed from the tree and returned.
// Otherwise, nil is returned.
func (t *Trie) ReplaceOrInsert(key []byte, property interface{}) *Node {
	if len(key) == 0 {
		return nil
	}
	node := t.root
	var pre *Node
	for _, k := range key {
		if n, ok := node.children[k]; ok {
			node = n
			pre = n
		} else {
			node = node.NewChildNode(k, nil, false)
			pre = nil
		}
	}
	// new node
	if pre == nil {
		node.property = property
		node.term = true
		t.size++
	} else {
		node = &Node{
			val:      pre.val,
			term:     pre.term,
			property: property,
			parent:   pre.parent,
			children: pre.children,
			depth:    pre.depth,
		}
		node.parent.ReplaceOrInsertChildNode(node)
	}
	return pre
}

// Finds and returns property data associated
// with `key`.
func (t *Trie) Find(key []byte) (*Node, bool) {
	node := findNode(t.Root(), key)
	if node == nil {
		return nil, false
	}
	if !node.term {
		return nil, false
	}

	return &Node{
		val:      node.Val(),
		parent:   node.Parent(),
		depth:    node.Depth(),
		term:     node.Terminating(),
		property: node.Property(),
	}, true
}

func (t *Trie) HasKeysWithPrefix(key []byte) bool {
	node := findNode(t.Root(), key)
	return node != nil
}

// Removes a key from the trie.
// Return delete node if exist
// Note make sure the key is not only a prefix
func (t *Trie) Delete(key []byte) *Node {
	node := findNode(t.Root(), key)
	var del *Node
	if node.term {
		t.size--
		del = node
		if len(node.children) > 0 {
			// we just flag the term
			node.term = false
		} else {
			// no children node, we need delete from parent node
			if node.Parent() != nil {
				node.parent.RemoveChild(node.Val())
				// check the parent if the node has no children nodes
				for n := node.Parent(); n != nil; n = n.Parent() {
					if n.term {
						break
					}
					if len(n.children) > 0 {
						break
					}
					if n.Parent() != nil {
						n.parent.RemoveChild(n.Val())
					}
				}
			}
		}
		return del
	} else {
		// not end node
		return nil
	}
}

// Returns all the keys currently stored in the trie.
func (t *Trie) Keys() [][]byte {
	var keys [][]byte
	iter := func(key []byte, val interface{}) bool {
		k := make([]byte, len(key))
		copy(k, key)
		keys = append(keys, k)
		return true
	}
	t.PrefixSearch(nil, iter)
	return keys
}

// Performs a prefix search against the keys in the trie.
// The key and value are only valid for the life of the iterator.
func (t *Trie) PrefixSearch(pre []byte, iter NodeIterator) {
	node := findNode(t.Root(), pre)
	if node == nil {
		return
	}

	preTraverse(node, pre, iter)
}

// Creates and returns a pointer to a new child for the node.
func (n *Node) NewChildNode(val byte, property interface{}, term bool) *Node {
	node := &Node{
		val:      val,
		term:     term,
		property: property,
		parent:   n,
		children: make(map[byte]*Node),
		depth:    n.depth + 1,
	}
	n.children[val] = node
	return node
}

func (n *Node) ReplaceOrInsertChildNode(node *Node) {
	n.children[node.Val()] = node
}

func (n *Node) RemoveChild(r byte) {
	delete(n.children, r)
}

// Returns the parent of this node.
func (n Node) Parent() *Node {
	return n.parent
}

// Returns the property information of this node.
func (n Node) Property() interface{} {
	return n.property
}

// Returns the children of this node.
func (n Node) Children() map[byte]*Node {
	return n.children
}

func (n Node) Terminating() bool {
	return n.term
}

func (n Node) Val() byte {
	return n.val
}

func (n Node) Depth() int {
	return n.depth
}

func findNode(node *Node, key []byte) *Node {
	if node == nil {
		return nil
	}

	if len(key) == 0 {
		return node
	}

	n, ok := node.Children()[key[0]]
	if !ok {
		return nil
	}

	var subKey []byte
	if len(key) > 1 {
		subKey = key[1:]
	} else {
		subKey = key[0:0]
	}

	return findNode(n, subKey)
}

// Preorder traverse trie
func preTraverse(node *Node, prefix []byte, iter NodeIterator) {
	if node == nil {
		return
	}
	if node.term {
		l := len(prefix)
		buffer := bufalloc.AllocBuffer(l)
		_, err := buffer.Write(prefix)
		if err != nil {
			fmt.Println("copy prefix failed ", err)
			return
		}
		if !iter(buffer.Bytes(), node.Property()) {
			bufalloc.FreeBuffer(buffer)
			return
		}
		bufalloc.FreeBuffer(buffer)
	}
	if len(node.Children()) == 0 {
		return
	}
	// sort key
	bs := make([]byte, 0, len(node.Children()))
	for val, _ := range node.children {
		bs = append(bs, val)
	}
	sort.Sort(ByBytes(bs))
	for _, c := range bs {
		if n, ok := node.children[c]; ok {
			preTraverse(n, append(prefix, n.val), iter)
		}
	}
}

