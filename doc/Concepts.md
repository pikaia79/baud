# Concepts

Entities/Objects/Documents 

Attributes/Fields, 

Literals/Values, 

Terms

Associations/BinaryRelations, 

Classes/Categories/Concepts, 

Predicates/Edges, 

Facts

InstanceOF, SubclassOf, IsA

KG = a set of facts = (Entities or Classes) X Predicates X (Entities or Classes or Literals), i.e. SPO. 


* Entity Identification

UID is unique within the entire graph


* Prediate Signature

1, predicate types include: 

entity to literal (attribute), 

entity to entity (association), 

entity to class (instanceOf), 

class to class (subclassOf)

2, one to one (o2o) or one to many (o2m)

3, for an attribute, its value type such as number, string, date, etc; its indexing policy

4, the user-given name

examples: attr-o2m-string-address, attr-o2o-int-age, asso-o2m-dir2ino-test

* Indexing

Attributes and edges can be indexed for fast search, and fulltext indexing is also the first-class citizen. 

* Association

* Class

* Space


