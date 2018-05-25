package topo

import "github.com/tiglabs/baudengine/proto/metapb"

type PsTopo struct {
    version Version
    *metapb.Node
}