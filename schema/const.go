package schema

//the constant Predicate
const (
	PredicateInstanceOf   = "InstanceOf"
	PredicateSubclassOf   = "SubclassOf"
	PredicateSuperclassOf = "SuperclassOf"
)

const (
	PredicateAttributePrefix   = "Attr-"
	PredicateAssociationPrefix = "Asso-"
)

//the object types
const (
	ObjTypeOID     = 0
	ObjTypeString  = 1
	ObjTypeInteger = 2
)

//the indexing policies
const (
	IndexingNone     = 0
	IndexingMatch    = 1
	IndexingFulltext = 2
	IndexingFuzzy    = 3
)
