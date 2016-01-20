/// The general protocol for objects that can be compared by primary key.
///
/// In order to adopt it, you can implement its two required functions.
///
/// Another option is to adopt the simpler PrimaryKeyEquatable protocol, or to
/// adopt MutableDatabasePersistable:
///
///     struct Person1 : PrimaryKeyEquatable {
///         let id: Int64?
///
///         func hasEqualPrimaryKey(other: Person) -> Bool {
///             if case (let id?, let otherId?) = (id, other.id) { return id == otherId }
///             return false
///         }
///     }
///
///     struct Person2 : MutableDatabasePersistable, RowEquatable {
///         let id: Int64?
///
///         static func databaseTableName() -> String { return "persons" }
///         var persistentDictionary: [String: DatabaseValueConvertible?] {
///             return ["id": id]
///         }
///     }
public protocol RowEquatable {
    /// Returns a function that compares two objects.
    static func primaryKeyComparator(db: Database) -> (Self, Self) -> Bool
    
    /// Compares the primary keys of *self* and *other*.
    ///
    /// The result must be false if either primary key is null.
    func hasEqualPrimaryKey(other: Self, inDatabase db: Database) -> Bool
}

/// A simple protocol that makes it easy to adopt RowEquatable:
///
///     struct Person : PrimaryKeyEquatable {
///         let id: Int64?
///
///         func hasEqualPrimaryKey(other: Person) -> Bool {
///             if case (let id?, let otherId?) = (id, other.id) { return id == otherId }
///             return false
///         }
///     }
public protocol PrimaryKeyEquatable: RowEquatable {
    // Returns true if self and *other* have the same non-null primary key.
    func hasEqualPrimaryKey(other: Self) -> Bool
}

public extension PrimaryKeyEquatable {
    static func primaryKeyComparator(db: Database) -> (Self, Self) -> Bool {
        return { $0.hasEqualPrimaryKey($1, inDatabase: db) }
    }
    
    func hasEqualPrimaryKey(other: Self, inDatabase db: Database) -> Bool {
        return hasEqualPrimaryKey(other)
    }
}


/// Free implementation of RowEquatable for MutableDatabasePersistable
///
///     struct Person: MutableDatabasePersistable, RowEquatable {
///         let id: Int64?
///
///         static func databaseTableName() -> String { return "persons" }
///         var persistentDictionary: [String: DatabaseValueConvertible?] {
///             return ["id": id]
///         }
///     }
extension MutableDatabasePersistable where Self: RowEquatable {
    static func primaryKeyComparator(db: Database) -> (Self, Self) -> Bool {
        let tableName = databaseTableName()
        let primaryKey = db.primaryKey(tableName)
        let columns = primaryKey.columns
        return { (o1, o2) -> Bool in
            let values1 = databaseValues(forColumns: columns, inDictionary: o1.persistentDictionary)
            guard values1.indexOf({ !$0.isNull }) != nil else { return false }
            let values2 = databaseValues(forColumns: columns, inDictionary: o2.persistentDictionary)
            guard values2.indexOf({ !$0.isNull }) != nil else { return false }
            return values1 == values2
        }
    }
    
    func hasEqualPrimaryKey(other: Self, inDatabase db: Database) -> Bool {
        return self.dynamicType.primaryKeyComparator(db)(self, other)
    }
}
